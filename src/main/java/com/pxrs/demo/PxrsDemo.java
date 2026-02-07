package com.pxrs.demo;

import com.pxrs.shared.ModuloPartitionStrategy;
import com.pxrs.shared.PartitionQueues;
import com.pxrs.shared.PartitionState;
import com.pxrs.shared.PxrsConfig;
import com.pxrs.consumer.ConsumerEngine;
import com.pxrs.consumer.SimpleConsumer;
import com.pxrs.coordination.ConsumerCoordinator;
import com.pxrs.coordination.PartitionManager;
import com.pxrs.producer.SimpleProducer;
import com.pxrs.store.EtcdRegistryStore;
import com.pxrs.store.InMemoryRegistryStore;
import com.pxrs.store.RegistryStore;

import java.util.ArrayList;
import java.util.List;

public class PxrsDemo {

    public static void main(String[] args) throws Exception {
        boolean useEtcd = false;
        int numPartitions = 8;

        for (String arg : args) {
            if ("--etcd".equals(arg)) {
                useEtcd = true;
            } else {
                try {
                    numPartitions = Integer.parseInt(arg);
                } catch (NumberFormatException ignored) {
                }
            }
        }

        PxrsConfig config = PxrsConfig.builder()
                .numPartitions(numPartitions)
                .rebalanceIntervalMs(2000)
                .leaseTtlSeconds(10)
                .build();

        System.out.println("=== PXRS Demo ===");
        System.out.println("Config: " + config);
        System.out.println("Store: " + (useEtcd ? "etcd" : "in-memory"));
        System.out.println();

        // 1. Create store
        RegistryStore store;
        if (useEtcd) {
            store = new EtcdRegistryStore(config);
        } else {
            store = new InMemoryRegistryStore(5000);
        }
        store.initialize(numPartitions);

        // 2. Create coordination
        PartitionManager partitionManager = new PartitionManager(store, numPartitions);
        ConsumerCoordinator coordinator = new ConsumerCoordinator(store, partitionManager, config);

        // 3. Create partition queues and producer
        PartitionQueues partitionQueues = new PartitionQueues(numPartitions);
        SimpleProducer producer = new SimpleProducer(new ModuloPartitionStrategy(), numPartitions, partitionQueues);

        // 4. Create 3 consumers with engines
        List<ConsumerEngine> engines = new ArrayList<>();
        List<SimpleConsumer> consumers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String consumerId = "consumer-" + (char) ('A' + i);
            SimpleConsumer consumer = new SimpleConsumer(consumerId, partitionQueues, producer, store);
            ConsumerEngine engine = new ConsumerEngine(consumer, store);
            consumers.add(consumer);
            engines.add(engine);
            coordinator.addConsumer(engine);
        }

        // 5. Initial rebalance to assign partitions and start consume threads
        System.out.println("--- Starting consumers ---");
        coordinator.triggerRebalance();
        Thread.sleep(500);

        printAssignments(store, numPartitions);

        // 6. Producer sends 100 messages — consumers take() instantly
        System.out.println("\n--- Sending 100 messages ---");
        for (int i = 0; i < 100; i++) {
            producer.send("account-" + i, "payload-" + i);
        }
        System.out.println("Messages sent. Partition buffer sizes:");
        for (int i = 0; i < numPartitions; i++) {
            System.out.println("  Partition " + i + ": " + producer.getLatestOffset(i) + " messages");
        }

        // 7. Wait for consumers to process
        System.out.println("\n--- Waiting for consumers to process ---");
        Thread.sleep(3000);

        // 8. Print stats
        System.out.println("\n--- Stats after processing ---");
        for (int i = 0; i < consumers.size(); i++) {
            SimpleConsumer c = consumers.get(i);
            List<Integer> assigned = coordinator.getAssignedPartitions(c.getConsumerId());
            System.out.println(c.getConsumerId() + ": processed=" + c.getMessagesProcessed() +
                    " partitions=" + assigned);
        }
        printCheckpoints(store, numPartitions);

        // 9. Stop one consumer — show rebalancing
        System.out.println("\n--- Stopping consumer-A (simulating crash) ---");
        coordinator.removeConsumer(engines.get(0));
        Thread.sleep(500);

        System.out.println("Triggering rebalance...");
        coordinator.triggerRebalance();
        Thread.sleep(1000);

        System.out.println("\n--- Assignments after rebalance ---");
        printAssignments(store, numPartitions);

        for (int i = 1; i < consumers.size(); i++) {
            SimpleConsumer c = consumers.get(i);
            List<Integer> assigned = coordinator.getAssignedPartitions(c.getConsumerId());
            System.out.println(c.getConsumerId() + ": processed=" + c.getMessagesProcessed() +
                    " partitions=" + assigned);
        }

        // 10. Send more messages to show resumed processing
        System.out.println("\n--- Sending 50 more messages ---");
        for (int i = 100; i < 150; i++) {
            producer.send("account-" + i, "payload-" + i);
        }
        Thread.sleep(2000);

        System.out.println("\n--- Final stats ---");
        for (int i = 1; i < consumers.size(); i++) {
            SimpleConsumer c = consumers.get(i);
            List<Integer> assigned = coordinator.getAssignedPartitions(c.getConsumerId());
            System.out.println(c.getConsumerId() + ": processed=" + c.getMessagesProcessed() +
                    " partitions=" + assigned);
        }
        printCheckpoints(store, numPartitions);

        // Shutdown
        System.out.println("\n--- Shutting down ---");
        for (int i = 1; i < engines.size(); i++) {
            coordinator.removeConsumer(engines.get(i));
        }
        coordinator.stop();
        store.close();
        System.out.println("Done.");
    }

    private static void printAssignments(RegistryStore store, int numPartitions) {
        System.out.println("Partition assignments:");
        for (int i = 0; i < numPartitions; i++) {
            PartitionState ps = store.getPartitionState(i);
            String owner = ps.getOwnerId().isEmpty() ? "(unowned)" : ps.getOwnerId();
            System.out.println("  Partition " + i + " → " + owner +
                    " (epoch=" + ps.getVersionEpoch() + ")");
        }
    }

    private static void printCheckpoints(RegistryStore store, int numPartitions) {
        System.out.println("Checkpoint positions:");
        for (int i = 0; i < numPartitions; i++) {
            System.out.println("  Partition " + i + ": offset=" + store.getCheckpoint(i));
        }
    }
}
