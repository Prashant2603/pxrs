package com.pxrs.consumer;

import com.pxrs.shared.Message;
import com.pxrs.shared.PartitionQueues;
import com.pxrs.shared.PartitionState;
import com.pxrs.producer.Producer;
import com.pxrs.store.RegistryStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleConsumer implements Consumer {

    private final String consumerId;
    private final PartitionQueues partitionQueues;
    private final Producer producer;
    private final RegistryStore store;
    private final Map<Integer, AtomicBoolean> activePartitions = new ConcurrentHashMap<>();
    private final AtomicInteger messagesProcessed = new AtomicInteger(0);

    public SimpleConsumer(String consumerId, PartitionQueues partitionQueues,
                          Producer producer, RegistryStore store) {
        this.consumerId = consumerId;
        this.partitionQueues = partitionQueues;
        this.producer = producer;
        this.store = store;
    }

    @Override
    public String getConsumerId() {
        return consumerId;
    }

    @Override
    public void subscribe(int partitionId, long checkpoint) {
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
            checkpoint(partitionId, offset);
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
                checkpoint(partitionId, offset);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void unsubscribe(int partitionId) {
        AtomicBoolean active = activePartitions.get(partitionId);
        if (active != null) {
            active.set(false);
        }
    }

    @Override
    public void checkpoint(int partitionId, long offset) {
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

    public int getMessagesProcessed() {
        return messagesProcessed.get();
    }

    private void consumeMessage(Message msg) {
        System.out.println("[" + consumerId + "] Processed: partition=" +
                msg.getPartitionId() + " key=" + msg.getPartitionKey() +
                " payload=" + msg.getPayload());
        messagesProcessed.incrementAndGet();
    }
}
