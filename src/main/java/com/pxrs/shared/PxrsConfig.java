package com.pxrs.shared;

public class PxrsConfig {

    private final int numPartitions;
    private final long leaseTtlSeconds;
    private final long rebalanceIntervalMs;
    private final String etcdEndpoints;
    private final String keyPrefix;
    private final String jdbcUrl;
    private final String jdbcUsername;
    private final String jdbcPassword;

    private PxrsConfig(Builder builder) {
        this.numPartitions = builder.numPartitions;
        this.leaseTtlSeconds = builder.leaseTtlSeconds;
        this.rebalanceIntervalMs = builder.rebalanceIntervalMs;
        this.etcdEndpoints = builder.etcdEndpoints;
        this.keyPrefix = builder.keyPrefix;
        this.jdbcUrl = builder.jdbcUrl;
        this.jdbcUsername = builder.jdbcUsername;
        this.jdbcPassword = builder.jdbcPassword;
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

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getJdbcUsername() {
        return jdbcUsername;
    }

    public String getJdbcPassword() {
        return jdbcPassword;
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
        private String jdbcUrl = "jdbc:oracle:thin:@localhost:1521/FREEPDB1";
        private String jdbcUsername = "pxrs";
        private String jdbcPassword = "pxrs";

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

        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder jdbcUsername(String jdbcUsername) {
            this.jdbcUsername = jdbcUsername;
            return this;
        }

        public Builder jdbcPassword(String jdbcPassword) {
            this.jdbcPassword = jdbcPassword;
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
                ", keyPrefix='" + keyPrefix + "'" +
                ", jdbcUrl='" + jdbcUrl + "'}";
    }
}
