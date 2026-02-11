package com.pxrs.coordination;

import com.pxrs.shared.PartitionState;
import com.pxrs.shared.PxrsConfig;
import com.pxrs.store.InMemoryRegistryStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class PartitionManagerTest {

    private InMemoryRegistryStore store;
    private PartitionManager manager;

    @Before
    public void setUp() {
        store = new InMemoryRegistryStore(500);
        store.initialize(8);
        PxrsConfig config = PxrsConfig.builder().numPartitions(8).build();
        manager = new PartitionManager(store, config);
    }

    @After
    public void tearDown() {
        store.close();
    }

    @Test
    public void testComputeFairAssignment_singleConsumer() {
        Map<String, List<Integer>> assignment = manager.computeFairAssignment(
                Arrays.asList("c1"), 8);
        assertEquals(1, assignment.size());
        assertEquals(8, assignment.get("c1").size());
    }

    @Test
    public void testComputeFairAssignment_evenSplit() {
        Map<String, List<Integer>> assignment = manager.computeFairAssignment(
                Arrays.asList("c1", "c2"), 8);
        assertEquals(4, assignment.get("c1").size());
        assertEquals(4, assignment.get("c2").size());
    }

    @Test
    public void testComputeFairAssignment_unevenSplit() {
        Map<String, List<Integer>> assignment = manager.computeFairAssignment(
                Arrays.asList("c1", "c2", "c3"), 8);
        // 8 / 3 = 2 base, 2 remainder → first 2 get 3, last gets 2
        assertEquals(3, assignment.get("c1").size());
        assertEquals(3, assignment.get("c2").size());
        assertEquals(2, assignment.get("c3").size());
    }

    @Test
    public void testComputeFairAssignment_moreConsumersThanPartitions() {
        Map<String, List<Integer>> assignment = manager.computeFairAssignment(
                Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"), 8);
        int total = 0;
        for (List<Integer> parts : assignment.values()) {
            total += parts.size();
        }
        assertEquals(8, total);
    }

    @Test
    public void testComputeFairAssignment_allPartitionsCovered() {
        Map<String, List<Integer>> assignment = manager.computeFairAssignment(
                Arrays.asList("c1", "c2", "c3"), 8);

        boolean[] covered = new boolean[8];
        for (List<Integer> parts : assignment.values()) {
            for (int p : parts) {
                assertFalse("Partition " + p + " assigned twice", covered[p]);
                covered[p] = true;
            }
        }
        for (int i = 0; i < 8; i++) {
            assertTrue("Partition " + i + " not assigned", covered[i]);
        }
    }

    @Test
    public void testComputeFairAssignment_noConsumers() {
        Map<String, List<Integer>> assignment = manager.computeFairAssignment(
                Arrays.asList(), 8);
        assertTrue(assignment.isEmpty());
    }

    @Test
    public void testRebalance_assignsPartitions() {
        store.registerConsumer("c1");
        store.registerConsumer("c2");

        manager.rebalance();

        List<PartitionState> c1Parts = store.getPartitionsOwnedBy("c1");
        List<PartitionState> c2Parts = store.getPartitionsOwnedBy("c2");
        assertEquals(4, c1Parts.size());
        assertEquals(4, c2Parts.size());
        assertEquals(0, store.getUnownedPartitions().size());
    }

    @Test
    public void testRebalance_afterConsumerLeaves() {
        store.registerConsumer("c1");
        store.registerConsumer("c2");
        manager.rebalance();

        // c2 leaves
        store.releaseAllPartitions("c2");
        store.deregisterConsumer("c2");
        manager.rebalance();

        List<PartitionState> c1Parts = store.getPartitionsOwnedBy("c1");
        assertEquals(8, c1Parts.size());
        assertEquals(0, store.getUnownedPartitions().size());
    }

    @Test
    public void testRebalance_afterConsumerJoins() {
        store.registerConsumer("c1");
        manager.rebalance();
        assertEquals(8, store.getPartitionsOwnedBy("c1").size());

        // c2 joins
        store.registerConsumer("c2");
        manager.rebalance();

        assertEquals(4, store.getPartitionsOwnedBy("c1").size());
        assertEquals(4, store.getPartitionsOwnedBy("c2").size());
    }

    @Test
    public void testReclaimExpiredPartitions() {
        store.registerConsumer("c1");
        store.claimPartition(0, "c1", 0);
        store.claimPartition(1, "c1", 0);

        // Simulate crash: deregister without releasing
        store.deregisterConsumer("c1");

        manager.reclaimExpiredPartitions();

        assertEquals(0, store.getPartitionsOwnedBy("c1").size());
        assertEquals(8, store.getUnownedPartitions().size());
    }

    @Test
    public void testRebalance_preservesCheckpoints() {
        store.registerConsumer("c1");
        manager.rebalance();

        // Set some checkpoints
        PartitionState ps0 = store.getPartitionState(0);
        store.updateCheckpoint(0, "c1", 42, ps0.getVersionEpoch());

        // c1 leaves, c2 joins
        store.releaseAllPartitions("c1");
        store.deregisterConsumer("c1");
        store.registerConsumer("c2");
        manager.rebalance();

        // Checkpoint should be preserved
        assertEquals(42, store.getCheckpoint(0));
    }

    @Test
    public void testRebalance_threeConsumers() {
        store.registerConsumer("c1");
        store.registerConsumer("c2");
        store.registerConsumer("c3");

        manager.rebalance();

        int c1 = store.getPartitionsOwnedBy("c1").size();
        int c2 = store.getPartitionsOwnedBy("c2").size();
        int c3 = store.getPartitionsOwnedBy("c3").size();

        assertEquals(8, c1 + c2 + c3);
        assertTrue("Each consumer should have at least 2 partitions", c1 >= 2 && c2 >= 2 && c3 >= 2);
        assertTrue("No consumer should have more than 3 partitions", c1 <= 3 && c2 <= 3 && c3 <= 3);
    }
}
