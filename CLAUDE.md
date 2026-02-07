# PXRS - Project Context

## What This Is

PXRS (Partitioned Registry Store) is a Java 17 / Maven project implementing virtual partitioning with database-backed checkpointing for safe parallel message consumption. It replaces a point-to-point single-global-checkpoint model with per-partition, per-consumer checkpoints and epoch-fenced ownership.

## Key Design Decisions

- **Language**: Java 17 (uses pattern matching for instanceof in ConsumerCoordinator)
- **Build**: Maven (`pom.xml` at project root)
- **Coordination backend**: etcd (via `jetcd-core:0.7.7`) as default, swappable via `RegistryStore` interface
- **Testing**: JUnit 4.13.2, 33 tests all passing
- **No Spring/framework dependencies** — pure Java with minimal deps
- **No polling**: consumers block on `BlockingQueue.take()` via `PartitionQueues` for instant message delivery
- **Push-based assignments**: coordinator diffs partition ownership and pushes assign/revoke to `ConsumerEngine`

## Architecture (Layered)

```
Producer.send() → buffer + PartitionQueues.put()
                                ↓
ConsumerEngine (thread per partition) → Consumer.subscribe() → PartitionQueues.take() (blocking)
                                ↑
ConsumerCoordinator → rebalance → pushAssignments() → engine.onPartitionAssigned/Revoked
                                ↑
PartitionManager → RegistryStore → Backend (etcd / ConcurrentHashMap)
```

**Dependency rule:**
```
Producer ──→ Shared ←── Consumer ←── Coordinator
               ↑            ↑            ↑
             Store ─────────┘────────────┘
```

## Project Layout

```
src/main/java/com/pxrs/
├── shared/
│   ├── PxrsConfig.java              — Builder-pattern config (numPartitions, leaseTtl, rebalanceInterval, etcdEndpoints, keyPrefix)
│   ├── Message.java                 — Immutable (id, partitionKey, partitionId, payload, timestamp)
│   ├── PartitionState.java          — Mutable with volatile fields (partitionId, ownerId, lastCheckpoint, versionEpoch, lastHeartbeat)
│   ├── ConsumerInfo.java            — Mutable (consumerId, registeredAt, lastSeen)
│   ├── PartitionStrategy.java       — Interface: int assignPartition(String key, int numPartitions)
│   ├── ModuloPartitionStrategy.java — Math.abs(key.hashCode() % numPartitions)
│   └── PartitionQueues.java         — Map<Integer, BlockingQueue<Message>> for instant producer→consumer delivery
├── store/
│   ├── RegistryStore.java           — THE core interface (lifecycle, consumer registry, partition state, CAS claim, release, checkpoint, zombie detection)
│   ├── InMemoryRegistryStore.java   — ConcurrentHashMap + synchronized blocks, heartbeat-based zombie detection
│   └── EtcdRegistryStore.java       — jetcd Txn CAS on modRevision, lease-based auto-expiry
├── producer/
│   ├── Producer.java                — Interface: send, getNextMessage, getLatestOffset
│   └── SimpleProducer.java          — In-memory buffer + PartitionQueues push on send()
├── consumer/
│   ├── Consumer.java                — Interface: getConsumerId, subscribe(partitionId, checkpoint), unsubscribe(partitionId), checkpoint(partitionId, offset)
│   ├── SimpleConsumer.java          — Blocking subscribe: replay from producer, then take() from PartitionQueues; epoch-fenced checkpoints
│   └── ConsumerEngine.java          — Thread-per-partition lifecycle: onPartitionAssigned spawns thread, onPartitionRevoked interrupts
├── coordination/
│   ├── PartitionManager.java        — Fair-share rebalance (P/C base + remainder), reclaimExpiredPartitions
│   └── ConsumerCoordinator.java     — Push-based: manages engines, diffs assignments, calls onPartitionAssigned/Revoked, handles heartbeats
└── demo/PxrsDemo.java               — 3 consumers, 8 partitions, 150 messages, crash + rebalance demo
```

## Critical Interfaces

### Consumer (decoupled — no infrastructure concerns)
- `getConsumerId()` → String
- `subscribe(partitionId, checkpoint)` → void (BLOCKING — runs consume loop)
- `unsubscribe(partitionId)` → void (signals subscribe to stop)
- `checkpoint(partitionId, offset)` → void (epoch-fenced persist)

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

### Blocking Subscribe (no polling)
- `subscribe(partitionId, checkpoint)` is BLOCKING — called by ConsumerEngine on a dedicated thread
- Phase 1 (replay): loops `producer.getNextMessage(partitionId, offset++)` until null
- Phase 2 (live): blocks on `partitionQueues.take(partitionId)` — instant delivery, no sleep/poll
- On `unsubscribe()`: AtomicBoolean flag set false, thread interrupted → subscribe returns

### ConsumerEngine (thread-per-partition)
- `onPartitionAssigned(partitionId)`: reads checkpoint from store, spawns daemon thread calling `consumer.subscribe()`
- `onPartitionRevoked(partitionId)`: calls `consumer.unsubscribe()`, interrupts thread, joins with timeout
- `stop()`: unsubscribes and interrupts all partition threads

### Push-Based Assignments (ConsumerCoordinator)
- `addConsumer(engine)`: registers in store, tracks engine
- `removeConsumer(engine)`: stops engine, releases partitions, deregisters
- `pushAssignments()`: after each rebalance, diffs current vs last assignment per engine
  - Revokes removed partitions first (engine.onPartitionRevoked)
  - Assigns new partitions second (engine.onPartitionAssigned)
- Periodic task: reclaimExpired → rebalance → pushAssignments → updateHeartbeats

### Epoch Fencing
- `versionEpoch` on PartitionState increments on every claim/release
- Checkpoint updates require matching epoch — stale consumer gets rejected (returns false)
- On failed checkpoint, consumer marks partition inactive → subscribe loop exits

### CAS Claiming
- **InMemory**: `synchronized` block checks epoch + unowned, then sets owner + bumps epoch
- **etcd**: Txn API with `Cmp(modRevision == expected)` → linearizable CAS via Raft

### Zombie Detection
- **InMemory**: `getExpiredPartitions()` checks if owner is deregistered OR heartbeat exceeds `heartbeatTimeoutMs`
- **etcd**: Consumer key attached to lease with TTL → process death = lease expiry = key deleted → partition owner not in active consumers

### Fair-Share Rebalance
- `base = numPartitions / numConsumers`, `remainder = numPartitions % numConsumers`
- First `remainder` consumers get `base + 1` partitions, rest get `base`
- Sorted consumer IDs for deterministic assignment
- Releases excess partitions first, then claims unowned ones

## How to Build and Run

```bash
mvn clean compile test          # Build + 33 tests
mvn exec:java -Dexec.mainClass=com.pxrs.demo.PxrsDemo              # In-memory demo
mvn exec:java -Dexec.mainClass=com.pxrs.demo.PxrsDemo -Dexec.args="--etcd"  # etcd demo (requires local etcd)
```

## Tests (33 total, all passing)

- `InMemoryRegistryStoreTest` (16) — CAS races with CountDownLatch, epoch fencing, checkpoint preservation, zombie detection (deregistered + stale heartbeat)
- `ModuloPartitionStrategyTest` (5) — range validation, determinism, distribution evenness
- `PartitionManagerTest` (12) — fair-share computation, rebalance on join/leave/crash, checkpoint preservation across ownership changes

## SimpleConsumer Subscribe Pattern

```
ConsumerEngine.onPartitionAssigned(partitionId):
  checkpoint = store.getCheckpoint(partitionId)
  spawn thread → consumer.subscribe(partitionId, checkpoint)

subscribe(partitionId, checkpoint):
  activePartitions.put(partitionId, AtomicBoolean(true))
  1. Replay: while active → producer.getNextMessage(partitionId, offset++) → consumeMessage → checkpoint
  2. Live:   while active → partitionQueues.take(partitionId) → consumeMessage → checkpoint
  3. On InterruptedException or active=false → return

unsubscribe(partitionId):
  activePartitions.get(partitionId).set(false)

checkpoint(partitionId, offset):
  epoch = store.getPartitionState(partitionId).getVersionEpoch()
  if !store.updateCheckpoint(partitionId, consumerId, offset, epoch) → mark inactive
```

## etcd Key Layout

```
/pxrs/partitions/{id}/state  → "ownerId|lastCheckpoint|versionEpoch|lastHeartbeat"
/pxrs/consumers/{consumerId} → "registeredAt" (attached to lease)
```

## Known Limitations / Future Work

- SimpleProducer is in-memory only (no persistence/replication)
- No EtcdRegistryStore tests (requires running etcd)
- No end-to-end integration tests for SimpleConsumer/ConsumerEngine
- etcd Watch API for real-time rebalance notifications not yet implemented
- No backpressure or batching in consumer subscribe loop
- `InMemoryRegistryStore.heartbeatTimeoutMs` is separate from `PxrsConfig.leaseTtlSeconds` (could be unified)
- ConsumerCoordinator uses Java 17 pattern matching: `if (store instanceof InMemoryRegistryStore memStore)`

## Dependencies

- `io.etcd:jetcd-core:0.7.7` (brings in gRPC, Netty, Protobuf transitively)
- `junit:junit:4.13.2` (test scope)
- `org.codehaus.mojo:exec-maven-plugin:3.1.0` (for demo execution)

## README

Full README with Mermaid diagrams at `README.md` covering architecture, all data flows, rebalancing, epoch fencing, crash recovery, store swappability, consumer lifecycle, and configuration.
