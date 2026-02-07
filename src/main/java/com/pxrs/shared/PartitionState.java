package com.pxrs.shared;

public class PartitionState {

    private final int partitionId;
    private volatile String ownerId;
    private volatile long lastCheckpoint;
    private volatile long versionEpoch;
    private volatile long lastHeartbeat;

    public PartitionState(int partitionId, String ownerId, long lastCheckpoint,
                          long versionEpoch, long lastHeartbeat) {
        this.partitionId = partitionId;
        this.ownerId = ownerId;
        this.lastCheckpoint = lastCheckpoint;
        this.versionEpoch = versionEpoch;
        this.lastHeartbeat = lastHeartbeat;
    }

    public PartitionState(int partitionId) {
        this(partitionId, "", 0, 0, 0);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public long getLastCheckpoint() {
        return lastCheckpoint;
    }

    public void setLastCheckpoint(long lastCheckpoint) {
        this.lastCheckpoint = lastCheckpoint;
    }

    public long getVersionEpoch() {
        return versionEpoch;
    }

    public void setVersionEpoch(long versionEpoch) {
        this.versionEpoch = versionEpoch;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(long lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    @Override
    public String toString() {
        return "PartitionState{partitionId=" + partitionId +
                ", ownerId='" + ownerId + "'" +
                ", lastCheckpoint=" + lastCheckpoint +
                ", versionEpoch=" + versionEpoch +
                ", lastHeartbeat=" + lastHeartbeat + "}";
    }
}
