package com.pxrs.consumer;

public interface Consumer {

    String getConsumerId();

    void initialize();

    void subscribe();

    void stop();

    // Coordinator hooks (called during rebalance)
    void onPartitionAssigned(int partitionId);

    void onPartitionRevoked(int partitionId);

    int getMessagesProcessed();
}
