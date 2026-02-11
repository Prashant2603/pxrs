package com.pxrs.consumer;

import java.util.Map;
import java.util.Set;

public interface Consumer {

    String getConsumerId();

    void onPartitionsAssigned(Map<Integer, Long> partitionCheckpoints);

    void onPartitionsRevoked(Set<Integer> partitions);

    void stop();

    int getMessagesProcessed();
}
