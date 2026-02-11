package com.pxrs.consumer;

import com.pxrs.shared.PartitionQueues;
import com.pxrs.producer.Producer;
import com.pxrs.coordination.ConsumerCoordinator;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class ConsumerFactory {

    private final PartitionQueues partitionQueues;
    private final Producer producer;
    private final ConsumerCoordinator coordinator;

    @Inject
    public ConsumerFactory(PartitionQueues partitionQueues, Producer producer,
                           ConsumerCoordinator coordinator) {
        this.partitionQueues = partitionQueues;
        this.producer = producer;
        this.coordinator = coordinator;
    }

    public SimpleConsumer create(String consumerId) {
        return new SimpleConsumer(consumerId, partitionQueues, producer, coordinator);
    }
}
