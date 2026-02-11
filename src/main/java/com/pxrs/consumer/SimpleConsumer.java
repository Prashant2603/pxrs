package com.pxrs.consumer;

import com.pxrs.shared.Message;
import com.pxrs.shared.PartitionQueues;
import com.pxrs.producer.Producer;
import com.pxrs.coordination.ConsumerCoordinator;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleConsumer implements Consumer {

    private final String consumerId;
    private final PartitionQueues partitionQueues;
    private final Producer producer;
    private final ConsumerCoordinator coordinator;
    private final Map<Integer, AtomicBoolean> activePartitions = new ConcurrentHashMap<>();
    private final Map<Integer, Thread> partitionThreads = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> partitionOffsets = new ConcurrentHashMap<>();
    private final AtomicInteger messagesProcessed = new AtomicInteger(0);

    public SimpleConsumer(String consumerId, PartitionQueues partitionQueues,
                          Producer producer, ConsumerCoordinator coordinator) {
        this.consumerId = consumerId;
        this.partitionQueues = partitionQueues;
        this.producer = producer;
        this.coordinator = coordinator;
    }

    @Override
    public String getConsumerId() {
        return consumerId;
    }

    @Override
    public void onPartitionsAssigned(Map<Integer, Long> partitionCheckpoints) {
        for (Map.Entry<Integer, Long> entry : partitionCheckpoints.entrySet()) {
            int partitionId = entry.getKey();
            long checkpoint = entry.getValue();
            partitionOffsets.put(partitionId, new AtomicLong(checkpoint));
            Thread thread = new Thread(
                    () -> consumeLoop(partitionId, checkpoint),
                    "consumer-" + consumerId + "-p" + partitionId
            );
            thread.setDaemon(true);
            partitionThreads.put(partitionId, thread);
            thread.start();
        }
    }

    @Override
    public void onPartitionsRevoked(Set<Integer> partitions) {
        // Stop threads for revoked partitions
        for (int partitionId : partitions) {
            AtomicBoolean active = activePartitions.get(partitionId);
            if (active != null) {
                active.set(false);
            }
        }
        for (int partitionId : partitions) {
            Thread thread = partitionThreads.remove(partitionId);
            if (thread != null) {
                thread.interrupt();
                try {
                    thread.join(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        // Commit final checkpoints to coordinator
        for (int partitionId : partitions) {
            AtomicLong offset = partitionOffsets.remove(partitionId);
            if (offset != null) {
                coordinator.commitCheckpoint(consumerId, partitionId, offset.get());
            }
            activePartitions.remove(partitionId);
        }
    }

    @Override
    public void stop() {
        // Stop all partition threads and commit final checkpoints
        Set<Integer> allPartitions = Set.copyOf(partitionThreads.keySet());
        for (int partitionId : allPartitions) {
            AtomicBoolean active = activePartitions.get(partitionId);
            if (active != null) {
                active.set(false);
            }
        }
        for (int partitionId : allPartitions) {
            Thread thread = partitionThreads.remove(partitionId);
            if (thread != null) {
                thread.interrupt();
                try {
                    thread.join(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            AtomicLong offset = partitionOffsets.remove(partitionId);
            if (offset != null) {
                coordinator.commitCheckpoint(consumerId, partitionId, offset.get());
            }
            activePartitions.remove(partitionId);
        }
        coordinator.deregister(this);
    }

    @Override
    public int getMessagesProcessed() {
        return messagesProcessed.get();
    }

    private void consumeLoop(int partitionId, long checkpoint) {
        AtomicBoolean active = new AtomicBoolean(true);
        activePartitions.put(partitionId, active);

        // Replay historical messages
        long offset = checkpoint;
        while (active.get()) {
            Message msg = producer.getNextMessage(partitionId, offset);
            if (msg == null) {
                break;
            }
            consumeMessage(msg);
            offset++;
            doCheckpoint(partitionId, offset);
            if (!active.get()) {
                return;
            }
        }

        // Live consumption from queue
        while (active.get()) {
            try {
                Message msg = partitionQueues.take(partitionId);
                if (!active.get()) {
                    return;
                }
                consumeMessage(msg);
                offset++;
                doCheckpoint(partitionId, offset);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void doCheckpoint(int partitionId, long offset) {
        AtomicLong tracked = partitionOffsets.get(partitionId);
        if (tracked != null) {
            tracked.set(offset);
        }
        boolean updated = coordinator.commitCheckpoint(consumerId, partitionId, offset);
        if (!updated) {
            // Epoch mismatch — lost ownership
            AtomicBoolean active = activePartitions.get(partitionId);
            if (active != null) {
                active.set(false);
            }
        }
    }

    private void consumeMessage(Message msg) {
        System.out.println("[" + consumerId + "] Processed: partition=" +
                msg.getPartitionId() + " key=" + msg.getPartitionKey() +
                " payload=" + msg.getPayload());
        messagesProcessed.incrementAndGet();
    }
}
