package com.pxrs.partition;

public interface PartitionStrategy {

    int assignPartition(String partitionKey, int numPartitions);
}
