package com.pxrs.partition;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ModuloPartitionStrategyTest {

    private ModuloPartitionStrategy strategy;

    @Before
    public void setUp() {
        strategy = new ModuloPartitionStrategy();
    }

    @Test
    public void testAssignPartitionReturnsValidRange() {
        int numPartitions = 8;
        for (int i = 0; i < 100; i++) {
            int partition = strategy.assignPartition("key-" + i, numPartitions);
            assertTrue("Partition should be >= 0", partition >= 0);
            assertTrue("Partition should be < numPartitions", partition < numPartitions);
        }
    }

    @Test
    public void testSameKeyAlwaysSamePartition() {
        int numPartitions = 16;
        int first = strategy.assignPartition("account-123", numPartitions);
        for (int i = 0; i < 100; i++) {
            assertEquals(first, strategy.assignPartition("account-123", numPartitions));
        }
    }

    @Test
    public void testDistributionAcrossPartitions() {
        int numPartitions = 8;
        int numKeys = 1000;
        Map<Integer, Integer> counts = new HashMap<>();

        for (int i = 0; i < numKeys; i++) {
            int partition = strategy.assignPartition("key-" + i, numPartitions);
            counts.merge(partition, 1, Integer::sum);
        }

        // All partitions should receive at least some messages
        assertEquals("All partitions should be used", numPartitions, counts.size());

        // Check reasonable distribution (each partition gets at least 5% of messages)
        int minExpected = numKeys / numPartitions / 4;
        for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
            assertTrue("Partition " + entry.getKey() + " has too few messages: " + entry.getValue(),
                    entry.getValue() > minExpected);
        }
    }

    @Test
    public void testSinglePartition() {
        // All keys should map to partition 0
        for (int i = 0; i < 50; i++) {
            assertEquals(0, strategy.assignPartition("key-" + i, 1));
        }
    }

    @Test
    public void testDifferentPartitionCounts() {
        String key = "test-key";
        // Just verify it doesn't throw and returns valid values
        for (int numPartitions = 1; numPartitions <= 100; numPartitions++) {
            int partition = strategy.assignPartition(key, numPartitions);
            assertTrue(partition >= 0 && partition < numPartitions);
        }
    }
}
