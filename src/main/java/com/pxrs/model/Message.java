package com.pxrs.model;

public final class Message {

    private final String id;
    private final String partitionKey;
    private final int partitionId;
    private final String payload;
    private final long timestamp;

    public Message(String id, String partitionKey, int partitionId, String payload, long timestamp) {
        this.id = id;
        this.partitionKey = partitionKey;
        this.partitionId = partitionId;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Message{id='" + id + "', partitionKey='" + partitionKey +
                "', partitionId=" + partitionId + ", payload='" + payload +
                "', timestamp=" + timestamp + "}";
    }
}
