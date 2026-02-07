package com.pxrs.consumer;

import com.pxrs.coordination.ConsumerCoordinator;
import com.pxrs.model.Message;
import com.pxrs.model.PartitionState;
import com.pxrs.producer.Producer;
import com.pxrs.store.InMemoryRegistryStore;
import com.pxrs.store.RegistryStore;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleConsumer implements Consumer {

    private final String consumerId;
    private final RegistryStore store;
    private final Producer producer;
    private final ConsumerCoordinator coordinator;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger messagesProcessed = new AtomicInteger(0);
    private Thread pollThread;

    public SimpleConsumer(String consumerId, RegistryStore store, Producer producer,
                          ConsumerCoordinator coordinator) {
        this.consumerId = consumerId;
        this.store = store;
        this.producer = producer;
        this.coordinator = coordinator;
    }

    @Override
    public String getConsumerId() {
        return consumerId;
    }

    @Override
    public void start() {
        coordinator.registerConsumer(consumerId);
        running.set(true);
        pollThread = new Thread(this::pollLoop, "consumer-" + consumerId);
        pollThread.setDaemon(true);
        pollThread.start();
    }

    @Override
    public void stop() {
        running.set(false);
        if (pollThread != null) {
            pollThread.interrupt();
            try {
                pollThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        store.releaseAllPartitions(consumerId);
        coordinator.deregisterConsumer(consumerId);
    }

    @Override
    public void processMessage(Message message) {
        System.out.println("[" + consumerId + "] Processed: partition=" +
                message.getPartitionId() + " key=" + message.getPartitionKey() +
                " payload=" + message.getPayload());
        messagesProcessed.incrementAndGet();
    }

    public int getMessagesProcessed() {
        return messagesProcessed.get();
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                List<PartitionState> myPartitions = store.getPartitionsOwnedBy(consumerId);
                for (PartitionState ps : myPartitions) {
                    if (!running.get()) break;
                    consumePartition(ps);
                }

                // Heartbeat for in-memory store
                if (store instanceof InMemoryRegistryStore memStore) {
                    for (PartitionState ps : myPartitions) {
                        memStore.updateHeartbeat(consumerId, ps.getPartitionId());
                    }
                }

                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void consumePartition(PartitionState ps) {
        long checkpoint = store.getCheckpoint(ps.getPartitionId());
        long epoch = ps.getVersionEpoch();

        while (running.get()) {
            Message msg = producer.getNextMessage(ps.getPartitionId(), checkpoint);
            if (msg == null) {
                break;
            }
            processMessage(msg);
            checkpoint++;
            boolean updated = store.updateCheckpoint(
                    ps.getPartitionId(), consumerId, checkpoint, epoch);
            if (!updated) {
                // Epoch changed — we lost ownership
                break;
            }
        }
    }
}
