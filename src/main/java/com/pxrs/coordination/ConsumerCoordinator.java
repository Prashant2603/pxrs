package com.pxrs.coordination;

import com.pxrs.shared.PxrsConfig;
import com.pxrs.shared.PartitionState;
import com.pxrs.consumer.ConsumerEngine;
import com.pxrs.store.InMemoryRegistryStore;
import com.pxrs.store.RegistryStore;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConsumerCoordinator {

    private final RegistryStore store;
    private final PartitionManager partitionManager;
    private final PxrsConfig config;
    private final Map<String, ConsumerEngine> engines = new ConcurrentHashMap<>();
    private final Map<String, Set<Integer>> lastAssignment = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduler;

    public ConsumerCoordinator(RegistryStore store, PartitionManager partitionManager, PxrsConfig config) {
        this.store = store;
        this.partitionManager = partitionManager;
        this.config = config;
    }

    public void start() {
        scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "pxrs-coordinator");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(() -> {
            try {
                partitionManager.reclaimExpiredPartitions();
                partitionManager.rebalance();
                pushAssignments();
                updateHeartbeats();
            } catch (Exception e) {
                System.err.println("[ConsumerCoordinator] Rebalance error: " + e.getMessage());
            }
        }, 0, config.getRebalanceIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
        }
    }

    public void addConsumer(ConsumerEngine engine) {
        String consumerId = engine.getConsumerId();
        store.registerConsumer(consumerId);
        engines.put(consumerId, engine);
        lastAssignment.put(consumerId, new HashSet<>());
    }

    public void removeConsumer(ConsumerEngine engine) {
        String consumerId = engine.getConsumerId();
        engine.stop();
        store.releaseAllPartitions(consumerId);
        store.deregisterConsumer(consumerId);
        engines.remove(consumerId);
        lastAssignment.remove(consumerId);
    }

    public void triggerRebalance() {
        partitionManager.reclaimExpiredPartitions();
        partitionManager.rebalance();
        pushAssignments();
    }

    public List<Integer> getAssignedPartitions(String consumerId) {
        List<PartitionState> partitions = store.getPartitionsOwnedBy(consumerId);
        List<Integer> ids = new ArrayList<>();
        for (PartitionState ps : partitions) {
            ids.add(ps.getPartitionId());
        }
        return ids;
    }

    private void pushAssignments() {
        for (Map.Entry<String, ConsumerEngine> entry : engines.entrySet()) {
            String consumerId = entry.getKey();
            ConsumerEngine engine = entry.getValue();

            Set<Integer> currentPartitions = new HashSet<>();
            for (PartitionState ps : store.getPartitionsOwnedBy(consumerId)) {
                currentPartitions.add(ps.getPartitionId());
            }

            Set<Integer> previousPartitions = lastAssignment.getOrDefault(consumerId, new HashSet<>());

            // Revoke partitions no longer assigned
            for (int partitionId : previousPartitions) {
                if (!currentPartitions.contains(partitionId)) {
                    engine.onPartitionRevoked(partitionId);
                }
            }

            // Assign newly added partitions
            for (int partitionId : currentPartitions) {
                if (!previousPartitions.contains(partitionId)) {
                    engine.onPartitionAssigned(partitionId);
                }
            }

            lastAssignment.put(consumerId, currentPartitions);
        }
    }

    private void updateHeartbeats() {
        if (store instanceof InMemoryRegistryStore memStore) {
            for (Map.Entry<String, ConsumerEngine> entry : engines.entrySet()) {
                String consumerId = entry.getKey();
                Set<Integer> partitions = lastAssignment.getOrDefault(consumerId, new HashSet<>());
                for (int partitionId : partitions) {
                    memStore.updateHeartbeat(consumerId, partitionId);
                }
            }
        }
    }
}
