package com.pxrs.coordination;

import com.pxrs.config.PxrsConfig;
import com.pxrs.model.PartitionState;
import com.pxrs.store.RegistryStore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConsumerCoordinator {

    private final RegistryStore store;
    private final PartitionManager partitionManager;
    private final PxrsConfig config;
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

    public void registerConsumer(String consumerId) {
        store.registerConsumer(consumerId);
    }

    public void deregisterConsumer(String consumerId) {
        store.deregisterConsumer(consumerId);
    }

    public List<Integer> getAssignedPartitions(String consumerId) {
        List<PartitionState> partitions = store.getPartitionsOwnedBy(consumerId);
        List<Integer> ids = new ArrayList<>();
        for (PartitionState ps : partitions) {
            ids.add(ps.getPartitionId());
        }
        return ids;
    }

    public void triggerRebalance() {
        partitionManager.reclaimExpiredPartitions();
        partitionManager.rebalance();
    }
}
