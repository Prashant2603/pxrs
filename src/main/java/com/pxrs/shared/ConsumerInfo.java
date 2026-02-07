package com.pxrs.shared;

public class ConsumerInfo {

    private final String consumerId;
    private final long registeredAt;
    private volatile long lastSeen;

    public ConsumerInfo(String consumerId, long registeredAt, long lastSeen) {
        this.consumerId = consumerId;
        this.registeredAt = registeredAt;
        this.lastSeen = lastSeen;
    }

    public ConsumerInfo(String consumerId) {
        this(consumerId, System.currentTimeMillis(), System.currentTimeMillis());
    }

    public String getConsumerId() {
        return consumerId;
    }

    public long getRegisteredAt() {
        return registeredAt;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(long lastSeen) {
        this.lastSeen = lastSeen;
    }

    @Override
    public String toString() {
        return "ConsumerInfo{consumerId='" + consumerId +
                "', registeredAt=" + registeredAt +
                ", lastSeen=" + lastSeen + "}";
    }
}
