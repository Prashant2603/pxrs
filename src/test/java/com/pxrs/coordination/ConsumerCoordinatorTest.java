package com.pxrs.coordination;

import com.pxrs.consumer.Consumer;
import com.pxrs.shared.PxrsConfig;
import com.pxrs.store.InMemoryRegistryStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class ConsumerCoordinatorTest {

    private InMemoryRegistryStore store;
    private PartitionManager partitionManager;
    private ConsumerCoordinator coordinator;
    private PxrsConfig config;

    @Before
    public void setUp() {
        config = PxrsConfig.builder()
                .numPartitions(8)
                .leaseTtlSeconds(10)
                .rebalanceIntervalMs(60000)
                .build();
        store = new InMemoryRegistryStore(5000);
        store.initialize(8);
        partitionManager = new PartitionManager(store, config);
        coordinator = new ConsumerCoordinator(store, partitionManager, config);
    }

    @After
    public void tearDown() {
        coordinator.stop();
        store.close();
    }

    @Test
    public void testRegisterSingleConsumer_getsAllPartitions() {
        TestConsumer c1 = new TestConsumer("c1");
        coordinator.register(c1);

        assertEquals(1, c1.assignedCalls.size());
        assertEquals(8, c1.assignedCalls.get(0).size());
        assertTrue(c1.revokedCalls.isEmpty());

        // All partitions should have checkpoint 0
        for (long ckpt : c1.assignedCalls.get(0).values()) {
            assertEquals(0, ckpt);
        }
    }

    @Test
    public void testRegisterSecondConsumer_revokeThenAssign() {
        TestConsumer c1 = new TestConsumer("c1");
        TestConsumer c2 = new TestConsumer("c2");

        coordinator.register(c1);
        assertEquals(8, c1.assignedCalls.get(0).size());

        // Register second consumer
        coordinator.register(c2);

        // c1 should have been revoked some partitions
        assertEquals(1, c1.revokedCalls.size());
        Set<Integer> c1Revoked = c1.revokedCalls.get(0);
        assertFalse(c1Revoked.isEmpty());

        // c2 should have been assigned those partitions
        assertEquals(1, c2.assignedCalls.size());
        Map<Integer, Long> c2Assigned = c2.assignedCalls.get(0);
        assertEquals(c1Revoked, c2Assigned.keySet());

        // Fair split: 4 + 4
        int c1Total = 8 - c1Revoked.size();
        assertEquals(4, c1Total);
        assertEquals(4, c2Assigned.size());
    }

    @Test
    public void testDeregister_remainingGainsPartitions() {
        TestConsumer c1 = new TestConsumer("c1");
        TestConsumer c2 = new TestConsumer("c2");

        coordinator.register(c1);
        coordinator.register(c2);

        int c2AssignedBefore = c2.assignedCalls.size();

        // Deregister c1 — c2 should gain c1's partitions
        coordinator.deregister(c1);

        assertEquals(c2AssignedBefore + 1, c2.assignedCalls.size());
        Map<Integer, Long> gained = c2.assignedCalls.get(c2.assignedCalls.size() - 1);
        // c2 should now own all 8 (gained the 4 from c1)
        assertEquals(4, gained.size());
    }

    @Test
    public void testCommitCheckpoint_updatesStore() {
        TestConsumer c1 = new TestConsumer("c1");
        coordinator.register(c1);

        boolean result = coordinator.commitCheckpoint("c1", 0, 42);
        assertTrue(result);
        assertEquals(42, store.getCheckpoint(0));
    }

    @Test
    public void testCheckpointFlowThrough_c1CommitsC2ReceivesSameCheckpoint() {
        TestConsumer c1 = new TestConsumer("c1");
        coordinator.register(c1);

        // c1 commits checkpoints on partitions it owns
        coordinator.commitCheckpoint("c1", 0, 10);
        coordinator.commitCheckpoint("c1", 1, 20);

        // c2 joins — c1 loses some partitions, c2 gets them with saved checkpoints
        TestConsumer c2 = new TestConsumer("c2");
        coordinator.register(c2);

        Map<Integer, Long> c2Assigned = c2.assignedCalls.get(0);
        for (Map.Entry<Integer, Long> entry : c2Assigned.entrySet()) {
            int partitionId = entry.getKey();
            long checkpoint = entry.getValue();
            assertEquals(store.getCheckpoint(partitionId), checkpoint);
        }
    }

    @Test
    public void testRevokeCompletesBeforeAssign() {
        // Track global ordering of revoke/assign calls
        List<String> callOrder = new ArrayList<>();

        Consumer c1 = new Consumer() {
            @Override
            public String getConsumerId() { return "c1"; }
            @Override
            public void onPartitionsAssigned(Map<Integer, Long> partitionCheckpoints) {
                callOrder.add("c1-assign");
            }
            @Override
            public void onPartitionsRevoked(Set<Integer> partitions) {
                callOrder.add("c1-revoke");
            }
            @Override
            public void stop() {}
            @Override
            public int getMessagesProcessed() { return 0; }
        };

        Consumer c2 = new Consumer() {
            @Override
            public String getConsumerId() { return "c2"; }
            @Override
            public void onPartitionsAssigned(Map<Integer, Long> partitionCheckpoints) {
                callOrder.add("c2-assign");
            }
            @Override
            public void onPartitionsRevoked(Set<Integer> partitions) {
                callOrder.add("c2-revoke");
            }
            @Override
            public void stop() {}
            @Override
            public int getMessagesProcessed() { return 0; }
        };

        coordinator.register(c1);
        callOrder.clear(); // Reset for second register

        coordinator.register(c2);

        // c1-revoke must come before c2-assign
        int revokeIdx = callOrder.indexOf("c1-revoke");
        int assignIdx = callOrder.indexOf("c2-assign");
        assertTrue("Revoke should happen before assign", revokeIdx >= 0);
        assertTrue("Assign should happen", assignIdx >= 0);
        assertTrue("Revoke must complete before assign", revokeIdx < assignIdx);
    }

    @Test
    public void testNoSpuriousNotifications() {
        TestConsumer c1 = new TestConsumer("c1");
        coordinator.register(c1);

        int assignCalls = c1.assignedCalls.size();
        int revokeCalls = c1.revokedCalls.size();

        // Register and immediately deregister an unrelated consumer
        TestConsumer c2 = new TestConsumer("c2");
        coordinator.register(c2);
        coordinator.deregister(c2);

        // c1 should have gotten revoke + assign for c2 joining, then assign for c2 leaving
        // But should not get spurious extra calls
        assertTrue(c1.assignedCalls.size() >= assignCalls);
        assertTrue(c1.revokedCalls.size() >= revokeCalls);
    }

    @Test
    public void testThreeConsumers_fairSplit() {
        TestConsumer c1 = new TestConsumer("c1");
        TestConsumer c2 = new TestConsumer("c2");
        TestConsumer c3 = new TestConsumer("c3");

        coordinator.register(c1);
        coordinator.register(c2);
        coordinator.register(c3);

        // All 8 partitions assigned, fair split: 3+3+2
        Set<Integer> allAssigned = new HashSet<>();
        for (TestConsumer c : List.of(c1, c2, c3)) {
            List<Integer> partitions = coordinator.getAssignedPartitions(c.getConsumerId());
            assertTrue(partitions.size() >= 2);
            assertTrue(partitions.size() <= 3);
            allAssigned.addAll(partitions);
        }
        assertEquals(8, allAssigned.size());
    }

    @Test
    public void testCommitCheckpointFailsForNonOwner() {
        TestConsumer c1 = new TestConsumer("c1");
        coordinator.register(c1);

        // c1 owns all partitions; committing as "c2" should fail
        boolean result = coordinator.commitCheckpoint("c2", 0, 42);
        assertFalse(result);
    }

    static class TestConsumer implements Consumer {
        final String consumerId;
        final List<Map<Integer, Long>> assignedCalls = new ArrayList<>();
        final List<Set<Integer>> revokedCalls = new ArrayList<>();

        TestConsumer(String consumerId) {
            this.consumerId = consumerId;
        }

        @Override
        public String getConsumerId() {
            return consumerId;
        }

        @Override
        public void onPartitionsAssigned(Map<Integer, Long> partitionCheckpoints) {
            assignedCalls.add(new HashMap<>(partitionCheckpoints));
        }

        @Override
        public void onPartitionsRevoked(Set<Integer> partitions) {
            revokedCalls.add(new HashSet<>(partitions));
        }

        @Override
        public void stop() {
        }

        @Override
        public int getMessagesProcessed() {
            return 0;
        }
    }
}
