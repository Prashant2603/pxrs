package com.pxrs.store;

import com.pxrs.shared.ConsumerInfo;
import com.pxrs.shared.PartitionState;

import java.util.List;

public interface RegistryStore {

    // Lifecycle
    void initialize(int numPartitions);

    void close();

    // Consumer registry
    void registerConsumer(String consumerId);

    void deregisterConsumer(String consumerId);

    List<ConsumerInfo> getActiveConsumers();

    // Partition state
    PartitionState getPartitionState(int partitionId);

    List<PartitionState> getAllPartitionStates();

    List<PartitionState> getUnownedPartitions();

    List<PartitionState> getPartitionsOwnedBy(String consumerId);

    // Atomic partition claiming (CAS on version-epoch)
    boolean claimPartition(int partitionId, String consumerId, long expectedEpoch);

    // Release ownership
    void releasePartition(int partitionId, String consumerId);

    void releaseAllPartitions(String consumerId);

    // Checkpointing (with epoch fencing)
    boolean updateCheckpoint(int partitionId, String consumerId, long checkpoint, long expectedEpoch);

    long getCheckpoint(int partitionId);

    // Zombie detection
    List<PartitionState> getExpiredPartitions();
}
