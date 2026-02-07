package com.pxrs.shared;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class PartitionQueues {

    private final Map<Integer, BlockingQueue<Message>> queues;

    public PartitionQueues(int numPartitions) {
        queues = new ConcurrentHashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            queues.put(i, new LinkedBlockingQueue<>());
        }
    }

    public void put(int partitionId, Message message) {
        BlockingQueue<Message> queue = queues.get(partitionId);
        if (queue != null) {
            queue.add(message);
        }
    }

    public Message take(int partitionId) throws InterruptedException {
        BlockingQueue<Message> queue = queues.get(partitionId);
        if (queue == null) {
            throw new IllegalArgumentException("No queue for partition " + partitionId);
        }
        return queue.take();
    }

    public BlockingQueue<Message> getQueue(int partitionId) {
        return queues.get(partitionId);
    }
}
