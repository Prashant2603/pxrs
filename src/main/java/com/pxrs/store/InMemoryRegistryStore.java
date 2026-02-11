package com.pxrs.store;

import com.pxrs.shared.ConsumerInfo;
import com.pxrs.shared.PartitionState;
import com.pxrs.shared.PxrsConfig;

import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryRegistryStore implements RegistryStore {

    private final ConcurrentHashMap<Integer, PartitionState> partitions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConsumerInfo> consumers = new ConcurrentHashMap<>();
    private final long heartbeatTimeoutMs;

    @Inject
    public InMemoryRegistryStore(PxrsConfig config) {
        this(config.getLeaseTtlSeconds() * 1000);
    }

    public InMemoryRegistryStore(long heartbeatTimeoutMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public InMemoryRegistryStore() {
        this(10_000);
    }

    @Override
    public void initialize(int numPartitions) {
        partitions.clear();
        for (int i = 0; i < numPartitions; i++) {
            partitions.put(i, new PartitionState(i));
        }
    }

    @Override
    public void close() {
        partitions.clear();
        consumers.clear();
    }

    @Override
    public void registerConsumer(String consumerId) {
        consumers.put(consumerId, new ConsumerInfo(consumerId));
    }

    @Override
    public void deregisterConsumer(String consumerId) {
        consumers.remove(consumerId);
    }

    @Override
    public List<ConsumerInfo> getActiveConsumers() {
        return new ArrayList<>(consumers.values());
    }

    @Override
    public PartitionState getPartitionState(int partitionId) {
        return partitions.get(partitionId);
    }

    @Override
    public List<PartitionState> getAllPartitionStates() {
        return new ArrayList<>(partitions.values());
    }

    @Override
    public List<PartitionState> getUnownedPartitions() {
        List<PartitionState> result = new ArrayList<>();
        for (PartitionState ps : partitions.values()) {
            if (ps.getOwnerId() == null || ps.getOwnerId().isEmpty()) {
                result.add(ps);
            }
        }
        return result;
    }

    @Override
    public List<PartitionState> getPartitionsOwnedBy(String consumerId) {
        List<PartitionState> result = new ArrayList<>();
        for (PartitionState ps : partitions.values()) {
            if (consumerId.equals(ps.getOwnerId())) {
                result.add(ps);
            }
        }
        return result;
    }

    @Override
    public synchronized boolean claimPartition(int partitionId, String consumerId, long expectedEpoch) {
        PartitionState ps = partitions.get(partitionId);
        if (ps == null) {
            return false;
        }
        if (ps.getVersionEpoch() != expectedEpoch) {
            return false;
        }
        if (ps.getOwnerId() != null && !ps.getOwnerId().isEmpty()) {
            return false;
        }
        ps.setOwnerId(consumerId);
        ps.setVersionEpoch(expectedEpoch + 1);
        ps.setLastHeartbeat(System.currentTimeMillis());
        return true;
    }

    @Override
    public synchronized void releasePartition(int partitionId, String consumerId) {
        PartitionState ps = partitions.get(partitionId);
        if (ps != null && consumerId.equals(ps.getOwnerId())) {
            ps.setOwnerId("");
            ps.setVersionEpoch(ps.getVersionEpoch() + 1);
            ps.setLastHeartbeat(0);
        }
    }

    @Override
    public synchronized void releaseAllPartitions(String consumerId) {
        for (PartitionState ps : partitions.values()) {
            if (consumerId.equals(ps.getOwnerId())) {
                ps.setOwnerId("");
                ps.setVersionEpoch(ps.getVersionEpoch() + 1);
                ps.setLastHeartbeat(0);
            }
        }
    }

    @Override
    public synchronized boolean updateCheckpoint(int partitionId, String consumerId,
                                                 long checkpoint, long expectedEpoch) {
        PartitionState ps = partitions.get(partitionId);
        if (ps == null) {
            return false;
        }
        if (ps.getVersionEpoch() != expectedEpoch) {
            return false;
        }
        if (!consumerId.equals(ps.getOwnerId())) {
            return false;
        }
        ps.setLastCheckpoint(checkpoint);
        ps.setLastHeartbeat(System.currentTimeMillis());
        return true;
    }

    @Override
    public long getCheckpoint(int partitionId) {
        PartitionState ps = partitions.get(partitionId);
        return ps != null ? ps.getLastCheckpoint() : 0;
    }

    @Override
    public synchronized List<PartitionState> getExpiredPartitions() {
        long now = System.currentTimeMillis();
        List<PartitionState> expired = new ArrayList<>();
        for (PartitionState ps : partitions.values()) {
            if (ps.getOwnerId() != null && !ps.getOwnerId().isEmpty()) {
                if (!consumers.containsKey(ps.getOwnerId())) {
                    expired.add(ps);
                } else if (ps.getLastHeartbeat() > 0 &&
                        (now - ps.getLastHeartbeat()) > heartbeatTimeoutMs) {
                    expired.add(ps);
                }
            }
        }
        return expired;
    }

    public void updateHeartbeat(String consumerId, int partitionId) {
        PartitionState ps = partitions.get(partitionId);
        if (ps != null && consumerId.equals(ps.getOwnerId())) {
            ps.setLastHeartbeat(System.currentTimeMillis());
        }
        ConsumerInfo ci = consumers.get(consumerId);
        if (ci != null) {
            ci.setLastSeen(System.currentTimeMillis());
        }
    }
}
