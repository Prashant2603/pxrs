package com.pxrs.producer;

import com.pxrs.model.Message;

public interface Producer {

    void send(String partitionKey, String payload);

    Message getNextMessage(int partitionId, long fromOffset);

    long getLatestOffset(int partitionId);
}
