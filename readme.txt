1. Problem: Limitations of Direct‑Pipe In‑Memory Queues

Current system uses a Point‑to‑Point (P2P) in‑memory or direct‑socket model. [xrs_proble...escription | PDF]
Only one global checkpoint/pointer, making multi‑consumer support impossible without conflicts. [xrs_proble...escription | PDF]
Adding a second consumer leads to duplicate messages or message stealing. [xrs_proble...escription | PDF]
No scalability: A single pipe cannot be split for parallel processing. [xrs_proble...escription | PDF]
Checkpoint collision: Consumers at different offsets confuse the producer; no way to track progress independently. [xrs_proble...escription | PDF]
Memory volatility: If the consumer crashes, in‑flight messages are lost; producer incorrectly believes delivery succeeded. [xrs_proble...escription | PDF]

Impact: System cannot scale to 10× throughput, cannot parallelize, and risks message loss.

2. Design Solution: PXRS (Partitioned XRS)
PXRS introduces virtual partitioning + database‑backed checkpointing to achieve safe parallel consumption.
2.1 Virtual Partitioning

Stream is logically divided into buckets (e.g., 0–15). [xrs_proble...escription | PDF]
Producer assigns a bucket_id per message (e.g., account_id % 16). [xrs_proble...escription | PDF]
Each bucket acts as an independent mini‑queue enabling parallelism.

2.2 SQL Registry (Oracle) as the Coordination Layer

Moves checkpointing out of memory into an Oracle table. [xrs_proble...escription | PDF]
Table tracks: BUCKET_ID, OWNER_ID, LAST_CHECKPOINT, VERSION_EPOCH, LAST_HEARTBEAT. [xrs_proble...escription | PDF]
Consumers become stateless, asking DB for the latest checkpoint and bucket ownership.

2.3 Multi‑Consumer Coordination
PXRS introduces three mechanisms to avoid conflicts:


Discovery / Lobby

New consumers register in a CONSUMER_REGISTRY, allowing fair share calculation (buckets ÷ consumers). [xrs_proble...escription | PDF]



Atomic Bucket Claiming

Update operation assigns an unowned bucket to a consumer.
Database ensures only one consumer wins ownership. [xrs_proble...escription | PDF]



Version‑Epoch Fencing

Each bucket has a VERSION_EPOCH.
Prevents stale "zombie" consumers from writing outdated checkpoints.
Ensures consistency during ownership transitions. [xrs_proble...escription | PDF]



2.4 Why PXRS Over Kafka?

Maintains existing high‑speed producer‑consumer implementation. [xrs_proble...escription | PDF]
No need to deploy/manage Kafka clusters; leverages existing Oracle infrastructure. [xrs_proble...escription | PDF]
Rebalancing happens per bucket, avoiding Kafka‑style global "rebalance storms." [xrs_proble...escription | PDF]


3. Final Outcome
PXRS transforms the system from a single‑threaded P2P pipeline into a parallel, fault‑tolerant, multi‑consumer architecture where:

Each consumer has an independent checkpoint.
Database arbitration eliminates conflicts.
Adding more consumers linearly increases throughput.
