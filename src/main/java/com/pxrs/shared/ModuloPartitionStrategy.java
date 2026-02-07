package com.pxrs.shared;

public class ModuloPartitionStrategy implements PartitionStrategy {

    @Override
    public int assignPartition(String partitionKey, int numPartitions) {
        return Math.abs(partitionKey.hashCode() % numPartitions);
    }
}
