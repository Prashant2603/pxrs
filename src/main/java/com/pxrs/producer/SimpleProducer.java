package com.pxrs.producer;

import com.pxrs.model.Message;
import com.pxrs.partition.PartitionStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleProducer implements Producer {

    private final PartitionStrategy strategy;
    private final int numPartitions;
    private final ConcurrentHashMap<Integer, List<Message>> partitionBuffers = new ConcurrentHashMap<>();

    public SimpleProducer(PartitionStrategy strategy, int numPartitions) {
        this.strategy = strategy;
        this.numPartitions = numPartitions;
        for (int i = 0; i < numPartitions; i++) {
            partitionBuffers.put(i, new ArrayList<>());
        }
    }

    @Override
    public synchronized void send(String partitionKey, String payload) {
        int partitionId = strategy.assignPartition(partitionKey, numPartitions);
        List<Message> buffer = partitionBuffers.get(partitionId);
        Message msg = new Message(
                UUID.randomUUID().toString(),
                partitionKey,
                partitionId,
                payload,
                System.currentTimeMillis()
        );
        buffer.add(msg);
    }

    @Override
    public synchronized Message getNextMessage(int partitionId, long fromOffset) {
        List<Message> buffer = partitionBuffers.get(partitionId);
        if (buffer == null) {
            return null;
        }
        int offset = (int) fromOffset;
        if (offset >= buffer.size()) {
            return null;
        }
        return buffer.get(offset);
    }

    @Override
    public synchronized long getLatestOffset(int partitionId) {
        List<Message> buffer = partitionBuffers.get(partitionId);
        return buffer != null ? buffer.size() : 0;
    }
}
