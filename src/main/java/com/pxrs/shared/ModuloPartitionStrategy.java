package com.pxrs.shared;

import com.google.inject.Inject;

public class ModuloPartitionStrategy implements PartitionStrategy {

    @Inject
    public ModuloPartitionStrategy() {
    }

    @Override
    public int assignPartition(String partitionKey, int numPartitions) {
        return Math.abs(partitionKey.hashCode() % numPartitions);
    }
}
