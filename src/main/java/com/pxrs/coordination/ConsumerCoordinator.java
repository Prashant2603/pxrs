package com.pxrs.coordination;

import com.pxrs.shared.PxrsConfig;
import com.pxrs.shared.PartitionState;
import com.pxrs.consumer.Consumer;
import com.pxrs.store.InMemoryRegistryStore;
import com.pxrs.store.OracleRegistryStore;
import com.pxrs.store.RegistryStore;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Singleton
public class ConsumerCoordinator {

    private final RegistryStore store;
    private final PartitionManager partitionManager;
    private final PxrsConfig config;
    private final Map<String, Consumer> consumers = new ConcurrentHashMap<>();
    private final Map<String, Set<Integer>> lastAssignment = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduler;

    @Inject
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
                updateHeartbeats();
                List<PartitionState> expired = store.getExpiredPartitions();
                if (!expired.isEmpty()) {
                    synchronized (this) {
                        partitionManager.reclaimExpiredPartitions();
                        rebalanceAndNotify();
                    }
                }
            } catch (Exception e) {
                System.err.println("[ConsumerCoordinator] Health monitor error: " + e.getMessage());
            }
        }, config.getRebalanceIntervalMs(), config.getRebalanceIntervalMs(), TimeUnit.MILLISECONDS);
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

    public synchronized void register(Consumer consumer) {
        String consumerId = consumer.getConsumerId();
        store.registerConsumer(consumerId);
        consumers.put(consumerId, consumer);
        lastAssignment.put(consumerId, new HashSet<>());

        partitionManager.reclaimExpiredPartitions();
        rebalanceAndNotify();
    }

    public synchronized void deregister(Consumer consumer) {
        String consumerId = consumer.getConsumerId();
        store.releaseAllPartitions(consumerId);
        store.deregisterConsumer(consumerId);
        consumers.remove(consumerId);
        lastAssignment.remove(consumerId);

        rebalanceAndNotify();
    }

    public boolean commitCheckpoint(String consumerId, int partitionId, long offset) {
        PartitionState ps = store.getPartitionState(partitionId);
        if (ps == null) {
            return false;
        }
        long epoch = ps.getVersionEpoch();
        return store.updateCheckpoint(partitionId, consumerId, offset, epoch);
    }

    public List<Integer> getAssignedPartitions(String consumerId) {
        List<PartitionState> partitions = store.getPartitionsOwnedBy(consumerId);
        List<Integer> ids = new ArrayList<>();
        for (PartitionState ps : partitions) {
            ids.add(ps.getPartitionId());
        }
        return ids;
    }

    private void rebalanceAndNotify() {
        partitionManager.rebalance();

        // Compute current store assignments per tracked consumer
        Map<String, Set<Integer>> currentAssignments = new HashMap<>();
        for (String consumerId : consumers.keySet()) {
            Set<Integer> owned = new HashSet<>();
            for (PartitionState ps : store.getPartitionsOwnedBy(consumerId)) {
                owned.add(ps.getPartitionId());
            }
            currentAssignments.put(consumerId, owned);
        }

        // Phase 1: revoke lost partitions from all consumers (synchronous — consumers commit checkpoints)
        Map<String, Set<Integer>> revokedPerConsumer = new HashMap<>();
        for (Map.Entry<String, Consumer> entry : consumers.entrySet()) {
            String consumerId = entry.getKey();
            Consumer consumer = entry.getValue();
            Set<Integer> previous = lastAssignment.getOrDefault(consumerId, new HashSet<>());
            Set<Integer> current = currentAssignments.getOrDefault(consumerId, new HashSet<>());

            Set<Integer> lost = new HashSet<>(previous);
            lost.removeAll(current);

            if (!lost.isEmpty()) {
                consumer.onPartitionsRevoked(lost);
                revokedPerConsumer.put(consumerId, lost);
                // Update lastAssignment immediately after revocation
                Set<Integer> updated = new HashSet<>(previous);
                updated.removeAll(lost);
                lastAssignment.put(consumerId, updated);
            }
        }

        // Phase 2: assign new partitions to all consumers (with checkpoints from store)
        for (Map.Entry<String, Consumer> entry : consumers.entrySet()) {
            String consumerId = entry.getKey();
            Consumer consumer = entry.getValue();
            Set<Integer> previous = lastAssignment.getOrDefault(consumerId, new HashSet<>());
            Set<Integer> current = currentAssignments.getOrDefault(consumerId, new HashSet<>());

            Set<Integer> gained = new HashSet<>(current);
            gained.removeAll(previous);

            if (!gained.isEmpty()) {
                Map<Integer, Long> partitionCheckpoints = new HashMap<>();
                for (int partitionId : gained) {
                    partitionCheckpoints.put(partitionId, store.getCheckpoint(partitionId));
                }
                consumer.onPartitionsAssigned(partitionCheckpoints);
            }

            lastAssignment.put(consumerId, current);
        }
    }

    private void updateHeartbeats() {
        if (store instanceof InMemoryRegistryStore memStore) {
            for (Map.Entry<String, Consumer> entry : consumers.entrySet()) {
                String consumerId = entry.getKey();
                Set<Integer> partitions = lastAssignment.getOrDefault(consumerId, new HashSet<>());
                for (int partitionId : partitions) {
                    memStore.updateHeartbeat(consumerId, partitionId);
                }
            }
        } else if (store instanceof OracleRegistryStore oracleStore) {
            for (Map.Entry<String, Consumer> entry : consumers.entrySet()) {
                String consumerId = entry.getKey();
                Set<Integer> partitions = lastAssignment.getOrDefault(consumerId, new HashSet<>());
                for (int partitionId : partitions) {
                    oracleStore.updateHeartbeat(consumerId, partitionId);
                }
            }
        }
    }
}
