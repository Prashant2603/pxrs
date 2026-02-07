package com.pxrs.store;

import com.pxrs.shared.ConsumerInfo;
import com.pxrs.shared.PartitionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class InMemoryRegistryStoreTest {

    private InMemoryRegistryStore store;

    @Before
    public void setUp() {
        store = new InMemoryRegistryStore(500);
        store.initialize(8);
    }

    @After
    public void tearDown() {
        store.close();
    }

    @Test
    public void testInitialize() {
        List<PartitionState> all = store.getAllPartitionStates();
        assertEquals(8, all.size());
        for (PartitionState ps : all) {
            assertEquals("", ps.getOwnerId());
            assertEquals(0, ps.getLastCheckpoint());
            assertEquals(0, ps.getVersionEpoch());
        }
    }

    @Test
    public void testRegisterAndDeregisterConsumer() {
        store.registerConsumer("c1");
        store.registerConsumer("c2");
        assertEquals(2, store.getActiveConsumers().size());

        store.deregisterConsumer("c1");
        assertEquals(1, store.getActiveConsumers().size());
        assertEquals("c2", store.getActiveConsumers().get(0).getConsumerId());
    }

    @Test
    public void testClaimPartition() {
        store.registerConsumer("c1");
        boolean claimed = store.claimPartition(0, "c1", 0);
        assertTrue(claimed);

        PartitionState ps = store.getPartitionState(0);
        assertEquals("c1", ps.getOwnerId());
        assertEquals(1, ps.getVersionEpoch());
    }

    @Test
    public void testClaimPartitionFailsOnWrongEpoch() {
        store.registerConsumer("c1");
        store.claimPartition(0, "c1", 0);

        // Try to claim with stale epoch
        store.registerConsumer("c2");
        boolean claimed = store.claimPartition(0, "c2", 0);
        assertFalse(claimed);
    }

    @Test
    public void testClaimPartitionFailsIfAlreadyOwned() {
        store.registerConsumer("c1");
        store.registerConsumer("c2");
        store.claimPartition(0, "c1", 0);

        // Partition is owned by c1, c2 can't claim it even with correct epoch
        boolean claimed = store.claimPartition(0, "c2", 1);
        assertFalse(claimed);
    }

    @Test
    public void testAtomicClaimingTwoThreadsRacing() throws InterruptedException {
        store.registerConsumer("c1");
        store.registerConsumer("c2");

        AtomicInteger c1Wins = new AtomicInteger(0);
        AtomicInteger c2Wins = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            try {
                startLatch.await();
                if (store.claimPartition(0, "c1", 0)) {
                    c1Wins.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                doneLatch.countDown();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                startLatch.await();
                if (store.claimPartition(0, "c2", 0)) {
                    c2Wins.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                doneLatch.countDown();
            }
        });

        t1.start();
        t2.start();
        startLatch.countDown();
        doneLatch.await();

        // Exactly one thread should win
        assertEquals(1, c1Wins.get() + c2Wins.get());

        PartitionState ps = store.getPartitionState(0);
        assertFalse(ps.getOwnerId().isEmpty());
    }

    @Test
    public void testReleasePartition() {
        store.registerConsumer("c1");
        store.claimPartition(0, "c1", 0);

        store.releasePartition(0, "c1");
        PartitionState ps = store.getPartitionState(0);
        assertEquals("", ps.getOwnerId());
        assertEquals(2, ps.getVersionEpoch()); // epoch bumped on claim and release
    }

    @Test
    public void testReleaseAllPartitions() {
        store.registerConsumer("c1");
        store.claimPartition(0, "c1", 0);
        store.claimPartition(1, "c1", 0);
        store.claimPartition(2, "c1", 0);

        store.releaseAllPartitions("c1");

        assertEquals(0, store.getPartitionsOwnedBy("c1").size());
        assertEquals(8, store.getUnownedPartitions().size());
    }

    @Test
    public void testUpdateCheckpoint() {
        store.registerConsumer("c1");
        store.claimPartition(0, "c1", 0);

        // Epoch is now 1 after claim
        boolean updated = store.updateCheckpoint(0, "c1", 5, 1);
        assertTrue(updated);
        assertEquals(5, store.getCheckpoint(0));
    }

    @Test
    public void testUpdateCheckpointFailsOnWrongEpoch() {
        store.registerConsumer("c1");
        store.claimPartition(0, "c1", 0);

        // Wrong epoch
        boolean updated = store.updateCheckpoint(0, "c1", 5, 0);
        assertFalse(updated);
    }

    @Test
    public void testUpdateCheckpointFailsForNonOwner() {
        store.registerConsumer("c1");
        store.registerConsumer("c2");
        store.claimPartition(0, "c1", 0);

        boolean updated = store.updateCheckpoint(0, "c2", 5, 1);
        assertFalse(updated);
    }

    @Test
    public void testGetExpiredPartitions_deregisteredConsumer() {
        store.registerConsumer("c1");
        store.claimPartition(0, "c1", 0);

        // Deregister consumer without releasing partitions
        store.deregisterConsumer("c1");

        List<PartitionState> expired = store.getExpiredPartitions();
        assertEquals(1, expired.size());
        assertEquals(0, expired.get(0).getPartitionId());
    }

    @Test
    public void testGetExpiredPartitions_staleHeartbeat() throws InterruptedException {
        store = new InMemoryRegistryStore(200); // 200ms timeout
        store.initialize(4);
        store.registerConsumer("c1");
        store.claimPartition(0, "c1", 0);

        // Wait for heartbeat to expire
        Thread.sleep(300);

        List<PartitionState> expired = store.getExpiredPartitions();
        assertEquals(1, expired.size());
        assertEquals(0, expired.get(0).getPartitionId());
    }

    @Test
    public void testGetUnownedPartitions() {
        store.registerConsumer("c1");
        store.claimPartition(0, "c1", 0);
        store.claimPartition(3, "c1", 0);

        List<PartitionState> unowned = store.getUnownedPartitions();
        assertEquals(6, unowned.size());
    }

    @Test
    public void testGetPartitionsOwnedBy() {
        store.registerConsumer("c1");
        store.registerConsumer("c2");
        store.claimPartition(0, "c1", 0);
        store.claimPartition(1, "c1", 0);
        store.claimPartition(2, "c2", 0);

        List<PartitionState> c1Parts = store.getPartitionsOwnedBy("c1");
        assertEquals(2, c1Parts.size());

        List<PartitionState> c2Parts = store.getPartitionsOwnedBy("c2");
        assertEquals(1, c2Parts.size());
    }

    @Test
    public void testCheckpointPreservedAfterRelease() {
        store.registerConsumer("c1");
        store.claimPartition(0, "c1", 0);
        store.updateCheckpoint(0, "c1", 10, 1);

        store.releasePartition(0, "c1");

        // Checkpoint should be preserved
        assertEquals(10, store.getCheckpoint(0));
    }
}
