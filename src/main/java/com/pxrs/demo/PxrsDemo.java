package com.pxrs.demo;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.pxrs.PxrsModule;
import com.pxrs.shared.PartitionState;
import com.pxrs.shared.PxrsConfig;
import com.pxrs.consumer.ConsumerFactory;
import com.pxrs.consumer.SimpleConsumer;
import com.pxrs.coordination.ConsumerCoordinator;
import com.pxrs.producer.Producer;
import com.pxrs.producer.SimpleProducer;
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

        // Create Guice injector
        Injector injector = Guice.createInjector(new PxrsModule(config, useEtcd));

        // Get singletons
        RegistryStore store = injector.getInstance(RegistryStore.class);
        store.initialize(config.getNumPartitions());

        ConsumerCoordinator coordinator = injector.getInstance(ConsumerCoordinator.class);
        coordinator.start();

        ConsumerFactory consumerFactory = injector.getInstance(ConsumerFactory.class);
        Producer producer = injector.getInstance(Producer.class);

        // Create 3 consumers via factory and register with coordinator
        List<SimpleConsumer> consumers = new ArrayList<>();
        System.out.println("--- Starting consumers ---");
        for (int i = 0; i < 3; i++) {
            String consumerId = "consumer-" + (char) ('A' + i);
            SimpleConsumer consumer = consumerFactory.create(consumerId);
            consumers.add(consumer);
            coordinator.register(consumer);
        }
        Thread.sleep(500);

        printAssignments(store, numPartitions);

        // Producer sends 100 messages — consumers take() instantly
        System.out.println("\n--- Sending 100 messages ---");
        for (int i = 0; i < 100; i++) {
            producer.send("account-" + i, "payload-" + i);
        }
        System.out.println("Messages sent. Partition buffer sizes:");
        SimpleProducer simpleProducer = (SimpleProducer) producer;
        for (int i = 0; i < numPartitions; i++) {
            System.out.println("  Partition " + i + ": " + simpleProducer.getLatestOffset(i) + " messages");
        }

        // Wait for consumers to process
        System.out.println("\n--- Waiting for consumers to process ---");
        Thread.sleep(3000);

        // Print stats
        System.out.println("\n--- Stats after processing ---");
        for (int i = 0; i < consumers.size(); i++) {
            SimpleConsumer c = consumers.get(i);
            List<Integer> assigned = coordinator.getAssignedPartitions(c.getConsumerId());
            System.out.println(c.getConsumerId() + ": processed=" + c.getMessagesProcessed() +
                    " partitions=" + assigned);
        }
        printCheckpoints(store, numPartitions);

        // Stop one consumer — show rebalancing
        System.out.println("\n--- Stopping consumer-A (simulating crash) ---");
        consumers.get(0).stop();
        Thread.sleep(1000);

        System.out.println("\n--- Assignments after rebalance ---");
        printAssignments(store, numPartitions);

        for (int i = 1; i < consumers.size(); i++) {
            SimpleConsumer c = consumers.get(i);
            List<Integer> assigned = coordinator.getAssignedPartitions(c.getConsumerId());
            System.out.println(c.getConsumerId() + ": processed=" + c.getMessagesProcessed() +
                    " partitions=" + assigned);
        }

        // Send more messages to show resumed processing
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
        for (int i = 1; i < consumers.size(); i++) {
            consumers.get(i).stop();
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
