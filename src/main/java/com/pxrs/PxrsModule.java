package com.pxrs;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.pxrs.coordination.ConsumerCoordinator;
import com.pxrs.coordination.PartitionManager;
import com.pxrs.producer.Producer;
import com.pxrs.producer.SimpleProducer;
import com.pxrs.shared.ModuloPartitionStrategy;
import com.pxrs.shared.PartitionQueues;
import com.pxrs.shared.PartitionStrategy;
import com.pxrs.shared.PxrsConfig;
import com.pxrs.store.EtcdRegistryStore;
import com.pxrs.store.InMemoryRegistryStore;
import com.pxrs.store.OracleRegistryStore;
import com.pxrs.store.RegistryStore;
import com.pxrs.store.StoreType;

public class PxrsModule extends AbstractModule {

    private final PxrsConfig config;
    private final StoreType storeType;

    public PxrsModule(PxrsConfig config, StoreType storeType) {
        this.config = config;
        this.storeType = storeType;
    }

    @Override
    protected void configure() {
        bind(PxrsConfig.class).toInstance(config);
        bind(PartitionStrategy.class).to(ModuloPartitionStrategy.class).in(Singleton.class);
        bind(PartitionQueues.class).in(Singleton.class);
        bind(PartitionManager.class).in(Singleton.class);
        bind(ConsumerCoordinator.class).in(Singleton.class);
        bind(Producer.class).to(SimpleProducer.class).in(Singleton.class);

        switch (storeType) {
            case IN_MEMORY -> bind(RegistryStore.class).to(InMemoryRegistryStore.class).in(Singleton.class);
            case ETCD -> bind(RegistryStore.class).to(EtcdRegistryStore.class).in(Singleton.class);
            case ORACLE -> bind(RegistryStore.class).to(OracleRegistryStore.class).in(Singleton.class);
        }
    }
}
