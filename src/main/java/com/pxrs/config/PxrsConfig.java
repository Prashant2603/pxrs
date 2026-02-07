package com.pxrs.config;

public class PxrsConfig {

    private final int numPartitions;
    private final long leaseTtlSeconds;
    private final long rebalanceIntervalMs;
    private final String etcdEndpoints;
    private final String keyPrefix;

    private PxrsConfig(Builder builder) {
        this.numPartitions = builder.numPartitions;
        this.leaseTtlSeconds = builder.leaseTtlSeconds;
        this.rebalanceIntervalMs = builder.rebalanceIntervalMs;
        this.etcdEndpoints = builder.etcdEndpoints;
        this.keyPrefix = builder.keyPrefix;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public long getLeaseTtlSeconds() {
        return leaseTtlSeconds;
    }

    public long getRebalanceIntervalMs() {
        return rebalanceIntervalMs;
    }

    public String getEtcdEndpoints() {
        return etcdEndpoints;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int numPartitions = 16;
        private long leaseTtlSeconds = 10;
        private long rebalanceIntervalMs = 10000;
        private String etcdEndpoints = "http://localhost:2379";
        private String keyPrefix = "/pxrs/";

        public Builder numPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        public Builder leaseTtlSeconds(long leaseTtlSeconds) {
            this.leaseTtlSeconds = leaseTtlSeconds;
            return this;
        }

        public Builder rebalanceIntervalMs(long rebalanceIntervalMs) {
            this.rebalanceIntervalMs = rebalanceIntervalMs;
            return this;
        }

        public Builder etcdEndpoints(String etcdEndpoints) {
            this.etcdEndpoints = etcdEndpoints;
            return this;
        }

        public Builder keyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
            return this;
        }

        public PxrsConfig build() {
            return new PxrsConfig(this);
        }
    }

    @Override
    public String toString() {
        return "PxrsConfig{numPartitions=" + numPartitions +
                ", leaseTtlSeconds=" + leaseTtlSeconds +
                ", rebalanceIntervalMs=" + rebalanceIntervalMs +
                ", etcdEndpoints='" + etcdEndpoints + "'" +
                ", keyPrefix='" + keyPrefix + "'}";
    }
}
