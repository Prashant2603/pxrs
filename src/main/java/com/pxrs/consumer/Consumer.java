package com.pxrs.consumer;

import com.pxrs.model.Message;

public interface Consumer {

    String getConsumerId();

    void start();

    void stop();

    void processMessage(Message message);
}
