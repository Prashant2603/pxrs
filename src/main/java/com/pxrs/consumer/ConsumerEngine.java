package com.pxrs.consumer;

import com.pxrs.store.RegistryStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerEngine {

    private final Consumer consumer;
    private final RegistryStore store;
    private final Map<Integer, Thread> partitionThreads = new ConcurrentHashMap<>();

    public ConsumerEngine(Consumer consumer, RegistryStore store) {
        this.consumer = consumer;
        this.store = store;
    }

    public void onPartitionAssigned(int partitionId) {
        long checkpoint = store.getCheckpoint(partitionId);
        Thread thread = new Thread(
                () -> consumer.subscribe(partitionId, checkpoint),
                "consumer-" + consumer.getConsumerId() + "-p" + partitionId
        );
        thread.setDaemon(true);
        partitionThreads.put(partitionId, thread);
        thread.start();
    }

    public void onPartitionRevoked(int partitionId) {
        consumer.unsubscribe(partitionId);
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

    public void stop() {
        for (int partitionId : partitionThreads.keySet()) {
            consumer.unsubscribe(partitionId);
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
    }

    public String getConsumerId() {
        return consumer.getConsumerId();
    }
}
