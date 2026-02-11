# PXRS - Project Context

## What This Is

PXRS (Partitioned Registry Store) is a Java 17 / Maven project implementing virtual partitioning with database-backed checkpointing for safe parallel message consumption. It replaces a point-to-point single-global-checkpoint model with per-partition, per-consumer checkpoints and epoch-fenced ownership.

## Key Design Decisions

- **Language**: Java 17 (uses pattern matching for instanceof in ConsumerCoordinator)
- **Build**: Maven (`pom.xml` at project root)
- **Coordination backend**: etcd (via `jetcd-core:0.7.7`) as default, swappable via `RegistryStore` interface
- **DI**: Guice 7.0.0 — `PxrsModule` wires all services, `ConsumerFactory` creates consumers with runtime IDs
- **Testing**: JUnit 4.13.2, 42 tests all passing
- **No polling**: consumers block on `BlockingQueue.take()` via `PartitionQueues` for instant message delivery
- **Coordinator-driven assignments**: coordinator is the single brain — registers consumers, computes fair-share, pushes revoke-then-assign with checkpoints
- **Event-driven rebalancing**: rebalance only on membership changes (register/deregister/crash), no periodic full rebalance
- **Consumers never touch the store**: checkpoints flow through coordinator (`commitCheckpoint`)

## Architecture (Layered)

```
Producer.send() → buffer + PartitionQueues.put()
                                ↓
SimpleConsumer (thread per partition) → consumeLoop() → PartitionQueues.take() (blocking)
     ↑ onPartitionsAssigned/Revoked     ↑ coordinator pushes Map<partitionId, checkpoint>
     ↓ commitCheckpoint(id, offset)     ↑
ConsumerCoordinator → register/deregister → rebalanceAndNotify()
     ↑ single brain                        ↑ revoke phase 1, assign phase 2
     ↓                                     ↑
PartitionManager → RegistryStore → Backend (etcd / ConcurrentHashMap)
```

**Dependency rule:**
```
Producer ──→ Shared ←── Consumer ←── Coordinator
               ↑                          ↑
             Store ──────────────────────┘
```
Consumer has NO direct store dependency — all checkpoint reads/writes go through coordinator.

## Project Layout

```
src/main/java/com/pxrs/
├── PxrsModule.java                    — Guice AbstractModule: wires config, store, coordinator, producer, queues
├── shared/
│   ├── PxrsConfig.java              — Builder-pattern config (numPartitions, leaseTtl, rebalanceInterval, etcdEndpoints, keyPrefix)
│   ├── Message.java                 — Immutable (id, partitionKey, partitionId, payload, timestamp)
│   ├── PartitionState.java          — Mutable with volatile fields (partitionId, ownerId, lastCheckpoint, versionEpoch, lastHeartbeat)
│   ├── ConsumerInfo.java            — Mutable (consumerId, registeredAt, lastSeen)
│   ├── PartitionStrategy.java       — Interface: int assignPartition(String key, int numPartitions)
│   ├── ModuloPartitionStrategy.java — Math.abs(key.hashCode() % numPartitions), @Inject no-arg
│   └── PartitionQueues.java         — Map<Integer, BlockingQueue<Message>>, @Inject from PxrsConfig
├── store/
│   ├── RegistryStore.java           — THE core interface (lifecycle, consumer registry, partition state, CAS claim, release, checkpoint, zombie detection)
│   ├── InMemoryRegistryStore.java   — ConcurrentHashMap + synchronized blocks, @Inject from PxrsConfig (derives heartbeat from leaseTtl)
│   └── EtcdRegistryStore.java       — jetcd Txn CAS on modRevision, lease-based auto-expiry, @Inject from PxrsConfig
├── producer/
│   ├── Producer.java                — Interface: send, getNextMessage, getLatestOffset
│   └── SimpleProducer.java          — In-memory buffer + PartitionQueues push on send(), @Inject from PxrsConfig
├── consumer/
│   ├── Consumer.java                — Interface: getConsumerId, onPartitionsAssigned(Map), onPartitionsRevoked(Set), stop, getMessagesProcessed
│   ├── SimpleConsumer.java          — Passive worker: coordinator-driven, checkpoints via coordinator.commitCheckpoint()
│   └── ConsumerFactory.java         — @Singleton factory: create(consumerId) → SimpleConsumer (not Guice-managed)
├── coordination/
│   ├── PartitionManager.java        — Fair-share rebalance (P/C base + remainder), reclaimExpiredPartitions, @Inject from PxrsConfig
│   └── ConsumerCoordinator.java     — The single brain: register/deregister/commitCheckpoint, event-driven rebalance, @Inject @Singleton
└── demo/PxrsDemo.java               — Guice injector, ConsumerFactory, 3 consumers, 8 partitions, 150 messages, crash + rebalance demo
```

## Critical Interfaces

### Consumer (coordinator-driven, passive worker)
- `getConsumerId()` → String
- `onPartitionsAssigned(Map<Integer, Long> partitionCheckpoints)` → void (start consuming from given checkpoints)
- `onPartitionsRevoked(Set<Integer> partitions)` → void (synchronous: finish work, commit final checkpoints, stop threads, return)
- `stop()` → void (full shutdown: stops threads, commits checkpoints, calls coordinator.deregister)
- `getMessagesProcessed()` → int

### ConsumerCoordinator (the single brain)
- `start()` → void (starts health monitor — heartbeats + zombie detection only)
- `stop()` → void (shuts down scheduler)
- `register(Consumer)` → void (synchronized: registers in store, rebalances, revoke-then-assign)
- `deregister(Consumer)` → void (synchronized: releases partitions, deregisters, rebalances remaining)
- `commitCheckpoint(consumerId, partitionId, offset)` → boolean (epoch-fenced, delegates to store)
- `getAssignedPartitions(consumerId)` → List<Integer>

### RegistryStore (the swappable coordination layer)
- `initialize(numPartitions)` / `close()`
- `registerConsumer(id)` / `deregisterConsumer(id)` / `getActiveConsumers()`
- `getPartitionState(id)` / `getAllPartitionStates()` / `getUnownedPartitions()` / `getPartitionsOwnedBy(consumerId)`
- `claimPartition(partitionId, consumerId, expectedEpoch)` → boolean (CAS)
- `releasePartition(partitionId, consumerId)` / `releaseAllPartitions(consumerId)`
- `updateCheckpoint(partitionId, consumerId, checkpoint, expectedEpoch)` → boolean (epoch-fenced)
- `getCheckpoint(partitionId)` → long
- `getExpiredPartitions()` → zombie detection

## Key Mechanisms

### Blocking Consume Loop (no polling)
- `consumeLoop(partitionId, checkpoint)` is BLOCKING — called internally on a dedicated thread per partition
- Phase 1 (replay): loops `producer.getNextMessage(partitionId, offset++)` until null
- Phase 2 (live): blocks on `partitionQueues.take(partitionId)` — instant delivery, no sleep/poll
- On `onPartitionsRevoked()`: AtomicBoolean flag set false, thread interrupted → consumeLoop returns

### Coordinator-Driven Consumer Lifecycle
- Consumer created via `ConsumerFactory.create(consumerId)` — NOT Guice-managed (runtime ID)
- `coordinator.register(consumer)`: registers in store → rebalance → revoke-then-assign → consumer.onPartitionsAssigned(Map<partitionId, checkpoint>)
- `consumer.onPartitionsAssigned(map)`: spawns daemon thread per partition running `consumeLoop(partitionId, checkpoint)` using provided checkpoints
- `consumer.onPartitionsRevoked(set)`: sets active=false, interrupts threads, joins (5s timeout), commits final checkpoints via `coordinator.commitCheckpoint()` — **synchronous, blocks until done**
- `consumer.stop()`: stops all threads, commits checkpoints, then calls `coordinator.deregister(this)` for store cleanup

### Event-Driven Rebalancing (ConsumerCoordinator)
- `register(consumer)` (synchronized): registerConsumer → reclaimExpired → rebalanceAndNotify()
- `deregister(consumer)` (synchronized): releaseAll → deregisterConsumer → rebalanceAndNotify()
- `rebalanceAndNotify()`: partitionManager.rebalance() → diff current vs lastAssignment per consumer
  - **Phase 1 (revoke)**: call `consumer.onPartitionsRevoked(lostPartitions)` for all consumers that lost partitions — synchronous, consumers commit final checkpoints during this call
  - **Phase 2 (assign)**: read checkpoints from store for gained partitions, call `consumer.onPartitionsAssigned(Map<partitionId, checkpoint>)` — consumers start from saved checkpoints
- Health monitor (periodic): updateHeartbeats + check getExpiredPartitions — only triggers rebalanceAndNotify when zombies actually detected

### Checkpoint Flow (through coordinator)
- Consumer calls `coordinator.commitCheckpoint(consumerId, partitionId, offset)` during normal processing and during revocation flush
- Coordinator delegates to `store.updateCheckpoint(partitionId, consumerId, offset, epoch)` with epoch-fencing
- On failure (epoch mismatch), consumer marks partition inactive — stops consuming that partition

### Epoch Fencing
- `versionEpoch` on PartitionState increments on every claim/release
- Checkpoint updates require matching epoch — stale consumer gets rejected (returns false)
- On failed checkpoint, consumer marks partition inactive → consume loop exits

### CAS Claiming
- **InMemory**: `synchronized` block checks epoch + unowned, then sets owner + bumps epoch
- **etcd**: Txn API with `Cmp(modRevision == expected)` → linearizable CAS via Raft

### Zombie Detection
- **InMemory**: `getExpiredPartitions()` checks if owner is deregistered OR heartbeat exceeds `heartbeatTimeoutMs` (derived from `leaseTtlSeconds * 1000`)
- **etcd**: Consumer key attached to lease with TTL → process death = lease expiry = key deleted → partition owner not in active consumers

### Fair-Share Rebalance
- `base = numPartitions / numConsumers`, `remainder = numPartitions % numConsumers`
- First `remainder` consumers get `base + 1` partitions, rest get `base`
- Sorted consumer IDs for deterministic assignment
- Releases excess partitions first, then claims unowned ones

## Guice Dependency Injection

### PxrsModule
- `PxrsConfig` bound as instance (pre-built)
- `PartitionStrategy` → `ModuloPartitionStrategy` (Singleton)
- `PartitionQueues` (Singleton, reads numPartitions from PxrsConfig)
- `PartitionManager` (Singleton, reads numPartitions from PxrsConfig)
- `ConsumerCoordinator` (Singleton)
- `Producer` → `SimpleProducer` (Singleton, reads numPartitions from PxrsConfig)
- `RegistryStore` → `InMemoryRegistryStore` or `EtcdRegistryStore` based on `useEtcd` flag

### ConsumerFactory
- @Singleton, @Inject constructor takes PartitionQueues, Producer, ConsumerCoordinator
- `create(consumerId)` returns `SimpleConsumer` — not Guice-managed (has runtime consumerId)

## How to Build and Run

```bash
mvn clean compile test          # Build + 42 tests
mvn exec:java -Dexec.mainClass=com.pxrs.demo.PxrsDemo              # In-memory demo
mvn exec:java -Dexec.mainClass=com.pxrs.demo.PxrsDemo -Dexec.args="--etcd"  # etcd demo (requires local etcd)
```

## Tests (42 total, all passing)

- `InMemoryRegistryStoreTest` (16) — CAS races with CountDownLatch, epoch fencing, checkpoint preservation, zombie detection (deregistered + stale heartbeat)
- `ModuloPartitionStrategyTest` (5) — range validation, determinism, distribution evenness
- `PartitionManagerTest` (12) — fair-share computation, rebalance on join/leave/crash, checkpoint preservation across ownership changes
- `ConsumerCoordinatorTest` (9) — register/deregister flows, revoke-before-assign ordering, checkpoint flow-through, fair-split verification, commitCheckpoint success/failure

## Consumer Lifecycle Pattern

```
// Setup (Guice)
Injector injector = Guice.createInjector(new PxrsModule(config, useEtcd));
ConsumerFactory factory = injector.getInstance(ConsumerFactory.class);
ConsumerCoordinator coordinator = injector.getInstance(ConsumerCoordinator.class);

// Create and register
SimpleConsumer consumer = factory.create("consumer-A");
coordinator.register(consumer)
  → store.registerConsumer("consumer-A")
  → reclaimExpired → rebalance → rebalanceAndNotify()
    → Phase 1: revoke lost from existing consumers (synchronous)
    → Phase 2: assign gained to all consumers with checkpoints
      → consumer.onPartitionsAssigned({0:0, 1:0, ..., 7:0})
        → spawn thread per partition → consumeLoop(partitionId, checkpoint)

consumeLoop(partitionId, checkpoint):
  activePartitions.put(partitionId, AtomicBoolean(true))
  1. Replay: while active → producer.getNextMessage(partitionId, offset++) → consumeMessage → doCheckpoint
  2. Live:   while active → partitionQueues.take(partitionId) → consumeMessage → doCheckpoint
  3. On InterruptedException or active=false → return

doCheckpoint(partitionId, offset):
  partitionOffsets.get(partitionId).set(offset)  // track locally for revocation flush
  if !coordinator.commitCheckpoint(consumerId, partitionId, offset) → mark inactive

// Shutdown
consumer.stop()
  → stop all partition threads, commit final checkpoints
  → coordinator.deregister(this)
    → store.releaseAllPartitions(id) + store.deregisterConsumer(id)
    → rebalanceAndNotify() for remaining consumers
```

## etcd Key Layout

```
/pxrs/partitions/{id}/state  → "ownerId|lastCheckpoint|versionEpoch|lastHeartbeat"
/pxrs/consumers/{consumerId} → "registeredAt" (attached to lease)
```

## Known Limitations / Future Work

- SimpleProducer is in-memory only (no persistence/replication)
- No EtcdRegistryStore tests (requires running etcd)
- No end-to-end integration tests for SimpleConsumer
- etcd Watch API for real-time rebalance notifications not yet implemented
- No backpressure or batching in consumer consume loop
- ConsumerCoordinator uses Java 17 pattern matching: `if (store instanceof InMemoryRegistryStore memStore)`

## Dependencies

- `io.etcd:jetcd-core:0.7.7` (brings in gRPC, Netty, Protobuf transitively)
- `com.google.inject:guice:7.0.0` (Guice DI)
- `junit:junit:4.13.2` (test scope)
- `org.codehaus.mojo:exec-maven-plugin:3.1.0` (for demo execution)

## README

Full README with Mermaid diagrams at `README.md` covering architecture, all data flows, rebalancing, epoch fencing, crash recovery, store swappability, consumer lifecycle, and configuration.
