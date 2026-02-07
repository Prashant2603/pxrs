package com.pxrs.consumer;

public interface Consumer {

    String getConsumerId();

    void subscribe(int partitionId, long checkpoint);

    void unsubscribe(int partitionId);

    void checkpoint(int partitionId, long offset);
}
