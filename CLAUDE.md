# PXRS - Project Context

## What This Is

PXRS (Partitioned Registry Store) is a Java 17 / Maven project implementing virtual partitioning with database-backed checkpointing for safe parallel message consumption. It replaces a point-to-point single-global-checkpoint model with per-partition, per-consumer checkpoints and epoch-fenced ownership.

## Key Design Decisions

- **Language**: Java 17 (uses pattern matching for instanceof in SimpleConsumer)
- **Build**: Maven (`pom.xml` at project root)
- **Coordination backend**: etcd (via `jetcd-core:0.7.7`) as default, swappable via `RegistryStore` interface
- **Testing**: JUnit 4.13.2, 33 tests all passing
- **No Spring/framework dependencies** — pure Java with minimal deps

## Architecture (Layered)

```
Consumer/Producer → Coordination (PartitionManager, ConsumerCoordinator) → RegistryStore → Backend (etcd / ConcurrentHashMap)
```

## Project Layout

```
src/main/java/com/pxrs/
├── config/PxrsConfig.java          — Builder-pattern config (numPartitions, leaseTtl, rebalanceInterval, etcdEndpoints, keyPrefix)
├── model/
│   ├── Message.java                — Immutable (id, partitionKey, partitionId, payload, timestamp)
│   ├── PartitionState.java         — Mutable with volatile fields (partitionId, ownerId, lastCheckpoint, versionEpoch, lastHeartbeat)
│   └── ConsumerInfo.java           — Mutable (consumerId, registeredAt, lastSeen)
├── partition/
│   ├── PartitionStrategy.java      — Interface: int assignPartition(String key, int numPartitions)
│   └── ModuloPartitionStrategy.java — Math.abs(key.hashCode() % numPartitions)
├── store/
│   ├── RegistryStore.java          — THE core interface (lifecycle, consumer registry, partition state, CAS claim, release, checkpoint, zombie detection)
│   ├── InMemoryRegistryStore.java  — ConcurrentHashMap + synchronized blocks, heartbeat-based zombie detection
│   └── EtcdRegistryStore.java      — jetcd Txn CAS on modRevision, lease-based auto-expiry
├── producer/
│   ├── Producer.java               — Interface: send, getNextMessage, getLatestOffset
│   └── SimpleProducer.java         — In-memory ConcurrentHashMap<Integer, List<Message>>, synchronized send
├── consumer/
│   ├── Consumer.java               — Interface: getConsumerId, start, stop, processMessage
│   └── SimpleConsumer.java         — Daemon poll thread, epoch-fenced checkpoint loop, 100ms poll interval
├── coordination/
│   ├── PartitionManager.java       — Fair-share rebalance (P/C base + remainder), reclaimExpiredPartitions
│   └── ConsumerCoordinator.java    — ScheduledExecutorService running rebalance periodically
└── demo/PxrsDemo.java              — 3 consumers, 8 partitions, 150 messages, crash + rebalance demo
```

## Critical Interfaces

### RegistryStore (the swappable layer)
- `initialize(numPartitions)` / `close()`
- `registerConsumer(id)` / `deregisterConsumer(id)` / `getActiveConsumers()`
- `getPartitionState(id)` / `getAllPartitionStates()` / `getUnownedPartitions()` / `getPartitionsOwnedBy(consumerId)`
- `claimPartition(partitionId, consumerId, expectedEpoch)` → boolean (CAS)
- `releasePartition(partitionId, consumerId)` / `releaseAllPartitions(consumerId)`
- `updateCheckpoint(partitionId, consumerId, checkpoint, expectedEpoch)` → boolean (epoch-fenced)
- `getCheckpoint(partitionId)` → long
- `getExpiredPartitions()` → zombie detection

## Key Mechanisms

### Epoch Fencing
- `versionEpoch` on PartitionState increments on every claim/release
- Checkpoint updates require matching epoch — stale consumer gets rejected (returns false)
- Consumer breaks poll loop on failed checkpoint → prevents dual-write

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

## SimpleConsumer Poll Loop Pattern

```
start() → registerConsumer + spawn daemon thread
poll loop (100ms sleep):
  1. getPartitionsOwnedBy(myId)
  2. For each partition:
     - checkpoint = getCheckpoint(partitionId)
     - epoch = ps.getVersionEpoch()
     - while msg = getNextMessage(partitionId, checkpoint):
         processMessage(msg)
         checkpoint++
         if !updateCheckpoint(partitionId, myId, checkpoint, epoch): break  ← epoch fence
  3. updateHeartbeat (InMemory only)
stop() → releaseAllPartitions + deregister
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
- No backpressure or batching in consumer poll loop
- `InMemoryRegistryStore.heartbeatTimeoutMs` is separate from `PxrsConfig.leaseTtlSeconds` (could be unified)
- SimpleConsumer uses Java 17 pattern matching: `if (store instanceof InMemoryRegistryStore memStore)`

## Dependencies

- `io.etcd:jetcd-core:0.7.7` (brings in gRPC, Netty, Protobuf transitively)
- `junit:junit:4.13.2` (test scope)
- `org.codehaus.mojo:exec-maven-plugin:3.1.0` (for demo execution)

## README

Full README with 11 Mermaid diagrams at `README.md` covering architecture, all data flows, rebalancing, epoch fencing, crash recovery, store swappability, consumer lifecycle, and configuration.
