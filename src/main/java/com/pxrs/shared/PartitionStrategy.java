package com.pxrs.shared;

public interface PartitionStrategy {

    int assignPartition(String partitionKey, int numPartitions);
}
