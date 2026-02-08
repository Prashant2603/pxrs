package com.pxrs.consumer;

import com.pxrs.shared.Message;
import com.pxrs.shared.PartitionQueues;
import com.pxrs.shared.PartitionState;
import com.pxrs.producer.Producer;
import com.pxrs.store.RegistryStore;
import com.pxrs.coordination.ConsumerCoordinator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleConsumer implements Consumer {

    private final String consumerId;
    private final PartitionQueues partitionQueues;
    private final Producer producer;
    private final RegistryStore store;
    private final ConsumerCoordinator coordinator;
    private final Map<Integer, AtomicBoolean> activePartitions = new ConcurrentHashMap<>();
    private final Map<Integer, Thread> partitionThreads = new ConcurrentHashMap<>();
    private final AtomicInteger messagesProcessed = new AtomicInteger(0);

    public SimpleConsumer(String consumerId, PartitionQueues partitionQueues,
                          Producer producer, RegistryStore store,
                          ConsumerCoordinator coordinator) {
        this.consumerId = consumerId;
        this.partitionQueues = partitionQueues;
        this.producer = producer;
        this.store = store;
        this.coordinator = coordinator;
    }

    @Override
    public String getConsumerId() {
        return consumerId;
    }

    @Override
    public void initialize() {
        coordinator.addConsumer(this);
    }

    @Override
    public void subscribe() {
        coordinator.triggerRebalance();
    }

    @Override
    public void stop() {
        for (int partitionId : partitionThreads.keySet()) {
            AtomicBoolean active = activePartitions.get(partitionId);
            if (active != null) {
                active.set(false);
            }
        }
        for (Map.Entry<Integer, Thread> entry : partitionThreads.entrySet()) {
            Thread thread = entry.getValue();
            thread.interrupt();
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        partitionThreads.clear();
        coordinator.removeConsumer(this);
    }

    @Override
    public void onPartitionAssigned(int partitionId) {
        long checkpoint = store.getCheckpoint(partitionId);
        Thread thread = new Thread(
                () -> consumeLoop(partitionId, checkpoint),
                "consumer-" + consumerId + "-p" + partitionId
        );
        thread.setDaemon(true);
        partitionThreads.put(partitionId, thread);
        thread.start();
    }

    @Override
    public void onPartitionRevoked(int partitionId) {
        AtomicBoolean active = activePartitions.get(partitionId);
        if (active != null) {
            active.set(false);
        }
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
        PartitionState ps = store.getPartitionState(partitionId);
        if (ps == null) {
            return;
        }
        long epoch = ps.getVersionEpoch();
        boolean updated = store.updateCheckpoint(partitionId, consumerId, offset, epoch);
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
