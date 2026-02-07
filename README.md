# PXRS - Partitioned Registry Store

A virtual partitioning framework with database-backed checkpointing for safe parallel message consumption. PXRS enables multiple consumers to process messages concurrently, each with independent checkpoints and epoch-fenced ownership, eliminating message loss and supporting dynamic scaling.

## Table of Contents

- [Problem](#problem)
- [Solution](#solution)
- [Architecture](#architecture)
- [Core Concepts](#core-concepts)
- [Data Flow](#data-flow)
- [Partition Assignment & Rebalancing](#partition-assignment--rebalancing)
- [Consumer Lifecycle](#consumer-lifecycle)
- [Epoch Fencing & Checkpoint Safety](#epoch-fencing--checkpoint-safety)
- [Crash Recovery](#crash-recovery)
- [Store Backend Swappability](#store-backend-swappability)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Testing](#testing)

---

## Problem

A point-to-point in-memory/direct-socket model with a single global checkpoint has several critical limitations:

- **No multi-consumer support** — only one consumer can process messages at a time
- **No parallel processing** — throughput is capped by a single consumer's speed
- **Message loss on crash** — a consumer crash loses all in-flight progress since the last global checkpoint
- **No partition isolation** — a slow message blocks all other messages in the system

## Solution

PXRS introduces **virtual partitioning** with **per-partition, per-consumer checkpointing** backed by a coordination store (etcd by default, swappable via the `RegistryStore` interface):

- Messages are deterministically assigned to partitions via a pluggable `PartitionStrategy`
- Each partition has an independent owner, checkpoint offset, and version epoch
- Consumers claim partitions atomically (CAS) and advance checkpoints with epoch fencing
- Dead consumers are detected automatically (lease expiry or heartbeat timeout) and their partitions are reclaimed and redistributed

---

## Architecture

```mermaid
graph TB
    subgraph Producers
        P[SimpleProducer]
    end

    subgraph "Partition Strategy"
        PS[ModuloPartitionStrategy]
    end

    subgraph "Message Buffers (per partition)"
        B0[Partition 0]
        B1[Partition 1]
        B2[Partition 2]
        BN[Partition N]
    end

    subgraph Coordination
        CC[ConsumerCoordinator]
        PM[PartitionManager]
    end

    subgraph "Registry Store (swappable)"
        RS{RegistryStore Interface}
        IM[InMemoryRegistryStore]
        ET[EtcdRegistryStore]
    end

    subgraph Consumers
        CA[Consumer A]
        CB[Consumer B]
        CX[Consumer C]
    end

    P -->|"send(key, payload)"| PS
    PS -->|partitionId| B0
    PS -->|partitionId| B1
    PS -->|partitionId| B2
    PS -->|partitionId| BN

    CC -->|"periodic rebalance"| PM
    PM -->|"claim / release / checkpoint"| RS
    RS --- IM
    RS --- ET

    CA -->|"poll owned partitions"| B0
    CA -->|"poll owned partitions"| B1
    CB -->|"poll owned partitions"| B2
    CX -->|"poll owned partitions"| BN

    CA -->|"updateCheckpoint"| RS
    CB -->|"updateCheckpoint"| RS
    CX -->|"updateCheckpoint"| RS
```

---

## Core Concepts

### Partition

A logical bucket to which messages are assigned. Each partition has independent state:

| Field | Description |
|-------|-------------|
| `partitionId` | Integer identifier (0 to N-1) |
| `ownerId` | Consumer currently responsible for this partition |
| `lastCheckpoint` | Offset of the last successfully processed message |
| `versionEpoch` | Monotonically increasing counter, bumped on every claim/release |
| `lastHeartbeat` | Timestamp of last activity (for zombie detection) |

### Epoch Fencing

Every partition carries a `versionEpoch`. Claim and release operations increment it. Checkpoint updates require the caller to supply the expected epoch — if it doesn't match, the update is rejected. This prevents a stale (fenced-off) consumer from overwriting a new owner's progress.

### Compare-And-Swap (CAS)

Partition claiming is atomic. In etcd, this uses the Txn API with `modRevision` comparison. In the in-memory store, `synchronized` blocks provide the same guarantee. Exactly one consumer wins when two race to claim the same partition.

---

## Data Flow

### Producer Send Flow

```mermaid
sequenceDiagram
    participant App as Application
    participant P as SimpleProducer
    participant S as PartitionStrategy
    participant B as Partition Buffer

    App->>P: send("acct-123", payload)
    P->>S: assignPartition("acct-123", 8)
    S-->>P: partitionId = 3
    P->>P: Create Message(id=UUID, key, partitionId, payload, timestamp)
    P->>B: Append to partition-3 buffer at offset N
```

### Consumer Poll & Checkpoint Flow

```mermaid
sequenceDiagram
    participant C as SimpleConsumer
    participant RS as RegistryStore
    participant P as SimpleProducer

    loop Every 100ms while running
        C->>RS: getPartitionsOwnedBy(consumerId)
        RS-->>C: [PartitionState(id=0, epoch=5), PartitionState(id=1, epoch=3)]

        loop For each owned partition
            C->>RS: getCheckpoint(partitionId)
            RS-->>C: offset = 42

            loop While messages available
                C->>P: getNextMessage(partitionId, offset)
                P-->>C: Message
                C->>C: processMessage(msg)
                C->>RS: updateCheckpoint(partitionId, consumerId, offset+1, epoch=5)
                alt Epoch matches
                    RS-->>C: true (checkpoint saved)
                else Epoch changed (lost ownership)
                    RS-->>C: false
                    C->>C: Break loop — stop processing this partition
                end
            end
        end

        C->>RS: updateHeartbeat(consumerId)
    end
```

### End-to-End Message Flow

```mermaid
flowchart LR
    subgraph "1. Produce"
        A["send('acct-42', data)"] --> B["hash('acct-42') % 8 = 3"]
        B --> C["Append to Partition 3<br/>offset = N"]
    end

    subgraph "2. Coordinate"
        D["Rebalance"] --> E["Consumer-A owns [0,1,2]<br/>Consumer-B owns [3,4,5]<br/>Consumer-C owns [6,7]"]
    end

    subgraph "3. Consume"
        F["Consumer-B polls partition 3"] --> G["Read from offset N"]
        G --> H["Process message"]
        H --> I["Checkpoint offset N+1<br/>(epoch fenced)"]
    end

    C -.->|"assigned to<br/>Consumer-B"| F
```

---

## Partition Assignment & Rebalancing

### Fair-Share Algorithm

Partitions are distributed evenly across active consumers. With `P` partitions and `C` consumers:

- Each consumer gets `floor(P / C)` partitions
- The first `P mod C` consumers each get one extra partition
- Partition IDs are assigned in sorted consumer order for determinism

```mermaid
flowchart TD
    A[Rebalance Triggered] --> B[Get active consumers from store]
    B --> C{Any consumers?}
    C -->|No| Z[Return — nothing to do]
    C -->|Yes| D[Sort consumer IDs]
    D --> E["Compute fair assignment<br/>base = P / C, remainder = P % C"]
    E --> F["Release partitions not in<br/>desired assignment"]
    F --> G["Claim unowned partitions<br/>per desired assignment (CAS)"]
    G --> H[Rebalance complete]

    style A fill:#e1f5fe
    style H fill:#e8f5e9
```

### Rebalance Examples

```mermaid
graph LR
    subgraph "3 Consumers, 8 Partitions"
        direction TB
        CA1["Consumer A<br/>Partitions: 0, 1, 2"]
        CB1["Consumer B<br/>Partitions: 3, 4, 5"]
        CC1["Consumer C<br/>Partitions: 6, 7"]
    end

    subgraph "After Consumer A Leaves"
        direction TB
        CB2["Consumer B<br/>Partitions: 0, 1, 2, 3"]
        CC2["Consumer C<br/>Partitions: 4, 5, 6, 7"]
    end

    CA1 -.->|"crash / stop"| CB2
    CB1 -.->|"rebalance"| CB2
    CC1 -.->|"rebalance"| CC2
```

---

## Consumer Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Registering: start()
    Registering --> Polling: registerConsumer() + spawn thread

    state Polling {
        [*] --> QueryPartitions
        QueryPartitions --> ConsumePartitions: getPartitionsOwnedBy()
        ConsumePartitions --> Heartbeat: process all available messages
        Heartbeat --> QueryPartitions: sleep 100ms
    }

    Polling --> Stopping: stop()
    Stopping --> [*]: releaseAllPartitions() + deregister

    note right of Polling
        Each iteration:
        1. Query owned partitions
        2. For each partition: read → process → checkpoint
        3. Update heartbeat
        4. Sleep 100ms
    end note
```

---

## Epoch Fencing & Checkpoint Safety

Epoch fencing prevents stale consumers from corrupting checkpoint state after losing partition ownership.

```mermaid
sequenceDiagram
    participant A as Consumer A (original owner)
    participant RS as RegistryStore
    participant PM as PartitionManager
    participant B as Consumer B (new owner)

    Note over A,RS: Consumer A owns partition 0 (epoch=5)
    A->>RS: updateCheckpoint(0, "A", offset=10, epoch=5)
    RS-->>A: true

    Note over PM: Rebalance triggered — A lost partition 0
    PM->>RS: releasePartition(0, "A")
    Note over RS: epoch bumped to 6, owner cleared

    PM->>RS: claimPartition(0, "B", epoch=6)
    Note over RS: epoch bumped to 7, owner = B

    B->>RS: getCheckpoint(0) → offset=10
    B->>B: Resume processing from offset 10

    Note over A: A still running (stale reference, epoch=5)
    A->>RS: updateCheckpoint(0, "A", offset=11, epoch=5)
    RS-->>A: false (epoch mismatch: 5 ≠ 7)
    Note over A: A detects lost ownership → breaks poll loop

    B->>RS: updateCheckpoint(0, "B", offset=11, epoch=7)
    RS-->>B: true
```

---

## Crash Recovery

### With InMemoryRegistryStore

```mermaid
flowchart TD
    A["Consumer A crashes<br/>(stop without clean shutdown)"] --> B["Heartbeat goes stale<br/>lastHeartbeat + timeout < now"]
    B --> C["PartitionManager.reclaimExpiredPartitions()"]
    C --> D["store.getExpiredPartitions()<br/>detects stale heartbeat OR<br/>deregistered consumer"]
    D --> E["releasePartition(id, 'A')<br/>epoch bumped, owner cleared"]
    E --> F["Next rebalance()"]
    F --> G["Remaining consumers<br/>claim orphaned partitions"]
    G --> H["New owner resumes from<br/>last checkpoint offset"]

    style A fill:#ffcdd2
    style H fill:#e8f5e9
```

### With EtcdRegistryStore

```mermaid
flowchart TD
    A["Consumer A process dies"] --> B["gRPC connection drops"]
    B --> C["etcd lease expires after TTL<br/>(default 10s)"]
    C --> D["Consumer key auto-deleted<br/>/pxrs/consumers/A"]
    D --> E["getExpiredPartitions()<br/>owner 'A' not in activeConsumers"]
    E --> F["releasePartition(id, 'A')<br/>epoch bumped, owner cleared"]
    F --> G["Next rebalance()"]
    G --> H["CAS claim by new consumer<br/>Txn: IF modRevision matches"]
    H --> I["New owner resumes from<br/>last checkpoint offset"]

    style A fill:#ffcdd2
    style I fill:#e8f5e9
```

---

## Store Backend Swappability

The `RegistryStore` interface is the central abstraction. All coordination — consumer registration, partition claiming, checkpointing, and zombie detection — flows through it. Swapping backends requires zero changes to producers, consumers, or coordination logic.

```mermaid
classDiagram
    class RegistryStore {
        <<interface>>
        +initialize(int numPartitions)
        +close()
        +registerConsumer(String consumerId)
        +deregisterConsumer(String consumerId)
        +getActiveConsumers() List~ConsumerInfo~
        +getPartitionState(int partitionId) PartitionState
        +getAllPartitionStates() List~PartitionState~
        +getUnownedPartitions() List~PartitionState~
        +getPartitionsOwnedBy(String consumerId) List~PartitionState~
        +claimPartition(int partitionId, String consumerId, long expectedEpoch) boolean
        +releasePartition(int partitionId, String consumerId)
        +releaseAllPartitions(String consumerId)
        +updateCheckpoint(int partitionId, String consumerId, long checkpoint, long expectedEpoch) boolean
        +getCheckpoint(int partitionId) long
        +getExpiredPartitions() List~PartitionState~
    }

    class InMemoryRegistryStore {
        -ConcurrentHashMap partitions
        -ConcurrentHashMap consumers
        -long heartbeatTimeoutMs
        +updateHeartbeat(String consumerId, int partitionId)
    }

    class EtcdRegistryStore {
        -Client client
        -KV kvClient
        -Lease leaseClient
        -long leaseId
    }

    class OracleRegistryStore {
        <<future>>
        UPDATE ... WHERE epoch = ?
    }

    class RedisRegistryStore {
        <<future>>
        WATCH / MULTI / EXEC
    }

    RegistryStore <|.. InMemoryRegistryStore
    RegistryStore <|.. EtcdRegistryStore
    RegistryStore <|.. OracleRegistryStore
    RegistryStore <|.. RedisRegistryStore
```

| Backend | Atomic Claiming | Heartbeat / Zombie Detection | Best For |
|---------|----------------|------------------------------|----------|
| **InMemory** | `synchronized` block | Timestamp comparison | Unit tests, single-JVM dev |
| **etcd** | Txn CAS on `modRevision` | Lease TTL auto-expiry | Distributed production |
| **Oracle/Postgres** | `UPDATE ... WHERE epoch=?` | Polling `last_heartbeat` column | Enterprise environments |
| **Redis** | `WATCH/MULTI/EXEC` or Lua | Key TTL expiry | High-throughput |

---

## Configuration

All tuning knobs are centralized in `PxrsConfig` using the builder pattern:

```java
PxrsConfig config = PxrsConfig.builder()
    .numPartitions(16)              // default: 16
    .leaseTtlSeconds(10)            // default: 10
    .rebalanceIntervalMs(10000)     // default: 10000
    .etcdEndpoints("http://localhost:2379")  // default
    .keyPrefix("/pxrs/")            // default
    .build();
```

| Option | Default | Description |
|--------|---------|-------------|
| `numPartitions` | 16 | Number of virtual partitions to distribute across consumers |
| `leaseTtlSeconds` | 10 | etcd lease TTL; consumer must renew within this window or be considered dead |
| `rebalanceIntervalMs` | 10000 | How often the coordinator checks for rebalancing (ms) |
| `etcdEndpoints` | `http://localhost:2379` | Comma-separated etcd cluster endpoints |
| `keyPrefix` | `/pxrs/` | Namespace in etcd for multi-tenant isolation |

---

## Project Structure

```
pxrs/
├── pom.xml
└── src/
    ├── main/java/com/pxrs/
    │   ├── config/
    │   │   └── PxrsConfig.java              # Centralized configuration (builder pattern)
    │   ├── model/
    │   │   ├── Message.java                 # Immutable message record
    │   │   ├── PartitionState.java          # Partition ownership + checkpoint state
    │   │   └── ConsumerInfo.java            # Consumer registration info
    │   ├── partition/
    │   │   ├── PartitionStrategy.java       # Interface: key → partitionId
    │   │   └── ModuloPartitionStrategy.java # Default: abs(hashCode % N)
    │   ├── store/
    │   │   ├── RegistryStore.java           # Interface — the swappable coordination layer
    │   │   ├── EtcdRegistryStore.java       # Production: etcd Txn CAS + lease TTL
    │   │   └── InMemoryRegistryStore.java   # Testing: ConcurrentHashMap + synchronized
    │   ├── producer/
    │   │   ├── Producer.java                # Interface: send, getNextMessage, getLatestOffset
    │   │   └── SimpleProducer.java          # In-memory partitioned message buffer
    │   ├── consumer/
    │   │   ├── Consumer.java                # Interface: start, stop, processMessage
    │   │   └── SimpleConsumer.java          # Poll loop with epoch-fenced checkpointing
    │   ├── coordination/
    │   │   ├── PartitionManager.java        # Fair-share assignment + expired reclamation
    │   │   └── ConsumerCoordinator.java     # Scheduled rebalance orchestrator
    │   └── demo/
    │       └── PxrsDemo.java                # Full end-to-end demonstration
    └── test/java/com/pxrs/
        ├── store/
        │   └── InMemoryRegistryStoreTest.java   # 16 tests: CAS races, epoch fencing, expiry
        ├── partition/
        │   └── ModuloPartitionStrategyTest.java # 5 tests: range, determinism, distribution
        └── coordination/
            └── PartitionManagerTest.java        # 12 tests: fair-share, rebalancing, reclaim
```

---

## Getting Started

### Prerequisites

- Java 17+
- Maven 3.8+
- (Optional) etcd for distributed mode

### Build

```bash
mvn clean compile
```

### Run Tests

```bash
mvn test
```

### Run Demo (In-Memory Store)

```bash
mvn exec:java -Dexec.mainClass=com.pxrs.demo.PxrsDemo
```

The demo will:

1. Create 8 partitions and start 3 consumers (A, B, C)
2. Assign partitions fairly: A gets [0,1,2], B gets [3,4,5], C gets [6,7]
3. Send 100 messages with keys `account-0` through `account-99`
4. Wait for consumers to process and print per-consumer stats
5. Stop Consumer A (simulating a crash)
6. Trigger rebalance — B and C pick up A's orphaned partitions
7. Send 50 more messages and verify resumed processing from checkpoints
8. Print final stats and shut down

### Run Demo with Custom Partition Count

```bash
mvn exec:java -Dexec.mainClass=com.pxrs.demo.PxrsDemo -Dexec.args="16"
```

### Run Demo with etcd

Start a local etcd instance first:

```bash
etcd --listen-client-urls http://localhost:2379 --advertise-client-urls http://localhost:2379
```

Then run:

```bash
mvn exec:java -Dexec.mainClass=com.pxrs.demo.PxrsDemo -Dexec.args="--etcd"
```

---

## Testing

33 tests covering three areas:

### InMemoryRegistryStoreTest (16 tests)

- Initialization: partition slots created with correct defaults
- Consumer registration and deregistration
- **Atomic claiming**: two threads racing on the same partition — exactly one wins
- Claim rejection on wrong epoch or already-owned partition
- Partition release with epoch bump
- **Epoch-fenced checkpointing**: stale epoch or non-owner rejected
- **Zombie detection**: deregistered consumer and stale heartbeat
- Checkpoint preservation across ownership changes

### ModuloPartitionStrategyTest (5 tests)

- Valid range: all results in `[0, numPartitions)`
- Determinism: same key always maps to same partition
- Even distribution across partitions (1000 keys, all partitions used, each > 5%)
- Single partition edge case
- Various partition counts (1 through 100)

### PartitionManagerTest (12 tests)

- Fair-share computation: single consumer, even split, uneven split, more consumers than partitions
- All partitions covered with no duplicates
- **Rebalancing**: initial assignment, after consumer leaves, after consumer joins
- **Expired reclamation**: dead consumer's partitions released and reassigned
- **Checkpoint preservation**: offsets survive rebalance across different owners
