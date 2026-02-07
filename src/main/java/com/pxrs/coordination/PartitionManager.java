package com.pxrs.coordination;

import com.pxrs.shared.ConsumerInfo;
import com.pxrs.shared.PartitionState;
import com.pxrs.store.RegistryStore;

import java.util.*;

public class PartitionManager {

    private final RegistryStore store;
    private final int numPartitions;

    public PartitionManager(RegistryStore store, int numPartitions) {
        this.store = store;
        this.numPartitions = numPartitions;
    }

    public synchronized void rebalance() {
        List<ConsumerInfo> consumers = store.getActiveConsumers();
        if (consumers.isEmpty()) {
            return;
        }

        List<String> consumerIds = new ArrayList<>();
        for (ConsumerInfo ci : consumers) {
            consumerIds.add(ci.getConsumerId());
        }
        Collections.sort(consumerIds);

        Map<String, List<Integer>> desiredAssignment = computeFairAssignment(consumerIds, numPartitions);

        // Build current assignment
        Map<String, Set<Integer>> currentAssignment = new HashMap<>();
        for (String cid : consumerIds) {
            currentAssignment.put(cid, new HashSet<>());
        }
        List<PartitionState> allStates = store.getAllPartitionStates();
        for (PartitionState ps : allStates) {
            String owner = ps.getOwnerId();
            if (owner != null && !owner.isEmpty() && currentAssignment.containsKey(owner)) {
                currentAssignment.get(owner).add(ps.getPartitionId());
            }
        }

        // Release partitions that shouldn't belong to their current owner
        for (PartitionState ps : allStates) {
            String owner = ps.getOwnerId();
            if (owner != null && !owner.isEmpty()) {
                List<Integer> desired = desiredAssignment.get(owner);
                if (desired == null || !desired.contains(ps.getPartitionId())) {
                    store.releasePartition(ps.getPartitionId(), owner);
                }
            }
        }

        // Claim partitions according to desired assignment
        for (Map.Entry<String, List<Integer>> entry : desiredAssignment.entrySet()) {
            String consumerId = entry.getKey();
            for (int partitionId : entry.getValue()) {
                PartitionState ps = store.getPartitionState(partitionId);
                if (ps.getOwnerId() == null || ps.getOwnerId().isEmpty()) {
                    store.claimPartition(partitionId, consumerId, ps.getVersionEpoch());
                }
            }
        }
    }

    public Map<String, List<Integer>> computeFairAssignment(List<String> consumerIds, int numPartitions) {
        Map<String, List<Integer>> assignment = new LinkedHashMap<>();
        if (consumerIds.isEmpty()) {
            return assignment;
        }

        for (String cid : consumerIds) {
            assignment.put(cid, new ArrayList<>());
        }

        int numConsumers = consumerIds.size();
        int base = numPartitions / numConsumers;
        int remainder = numPartitions % numConsumers;

        int partitionIndex = 0;
        for (int i = 0; i < numConsumers; i++) {
            String consumerId = consumerIds.get(i);
            int count = base + (i < remainder ? 1 : 0);
            for (int j = 0; j < count; j++) {
                assignment.get(consumerId).add(partitionIndex++);
            }
        }

        return assignment;
    }

    public void reclaimExpiredPartitions() {
        List<PartitionState> expired = store.getExpiredPartitions();
        for (PartitionState ps : expired) {
            System.out.println("[PartitionManager] Reclaiming expired partition " +
                    ps.getPartitionId() + " from " + ps.getOwnerId());
            store.releasePartition(ps.getPartitionId(), ps.getOwnerId());
        }
    }
}
