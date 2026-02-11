package com.pxrs.store;

import com.pxrs.shared.ConsumerInfo;
import com.pxrs.shared.PartitionState;
import com.pxrs.shared.PxrsConfig;

import com.google.inject.Inject;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class OracleRegistryStore implements RegistryStore {

    private final String jdbcUrl;
    private final String jdbcUsername;
    private final String jdbcPassword;
    private final long heartbeatTimeoutMs;
    private Connection connection;

    @Inject
    public OracleRegistryStore(PxrsConfig config) {
        this.jdbcUrl = config.getJdbcUrl();
        this.jdbcUsername = config.getJdbcUsername();
        this.jdbcPassword = config.getJdbcPassword();
        this.heartbeatTimeoutMs = config.getLeaseTtlSeconds() * 1000;
    }

    @Override
    public void initialize(int numPartitions) {
        try {
            connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            connection.setAutoCommit(true);

            // Idempotently create partition rows using MERGE
            String mergeSql = "MERGE INTO pxrs_partitions p " +
                    "USING (SELECT ? AS partition_id FROM dual) src " +
                    "ON (p.partition_id = src.partition_id) " +
                    "WHEN NOT MATCHED THEN INSERT (partition_id, owner_id, last_checkpoint, version_epoch, last_heartbeat) " +
                    "VALUES (src.partition_id, '', 0, 0, 0)";
            try (PreparedStatement ps = connection.prepareStatement(mergeSql)) {
                for (int i = 0; i < numPartitions; i++) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize OracleRegistryStore", e);
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to close OracleRegistryStore", e);
            }
        }
    }

    @Override
    public void registerConsumer(String consumerId) {
        try {
            String sql = "MERGE INTO pxrs_consumers c " +
                    "USING (SELECT ? AS consumer_id FROM dual) src " +
                    "ON (c.consumer_id = src.consumer_id) " +
                    "WHEN MATCHED THEN UPDATE SET last_seen = ? " +
                    "WHEN NOT MATCHED THEN INSERT (consumer_id, registered_at, last_seen) " +
                    "VALUES (src.consumer_id, ?, ?)";
            long now = System.currentTimeMillis();
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, consumerId);
                ps.setLong(2, now);
                ps.setLong(3, now);
                ps.setLong(4, now);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register consumer: " + consumerId, e);
        }
    }

    @Override
    public void deregisterConsumer(String consumerId) {
        try {
            String sql = "DELETE FROM pxrs_consumers WHERE consumer_id = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, consumerId);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to deregister consumer: " + consumerId, e);
        }
    }

    @Override
    public List<ConsumerInfo> getActiveConsumers() {
        try {
            String sql = "SELECT consumer_id, registered_at, last_seen FROM pxrs_consumers";
            List<ConsumerInfo> result = new ArrayList<>();
            try (PreparedStatement ps = connection.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new ConsumerInfo(
                            rs.getString("consumer_id"),
                            rs.getLong("registered_at"),
                            rs.getLong("last_seen")));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get active consumers", e);
        }
    }

    @Override
    public PartitionState getPartitionState(int partitionId) {
        try {
            String sql = "SELECT partition_id, owner_id, last_checkpoint, version_epoch, last_heartbeat " +
                    "FROM pxrs_partitions WHERE partition_id = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setInt(1, partitionId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return mapPartitionState(rs);
                    }
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get partition state: " + partitionId, e);
        }
    }

    @Override
    public List<PartitionState> getAllPartitionStates() {
        try {
            String sql = "SELECT partition_id, owner_id, last_checkpoint, version_epoch, last_heartbeat " +
                    "FROM pxrs_partitions ORDER BY partition_id";
            List<PartitionState> result = new ArrayList<>();
            try (PreparedStatement ps = connection.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(mapPartitionState(rs));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get all partition states", e);
        }
    }

    @Override
    public List<PartitionState> getUnownedPartitions() {
        try {
            String sql = "SELECT partition_id, owner_id, last_checkpoint, version_epoch, last_heartbeat " +
                    "FROM pxrs_partitions WHERE owner_id = '' OR owner_id IS NULL";
            List<PartitionState> result = new ArrayList<>();
            try (PreparedStatement ps = connection.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(mapPartitionState(rs));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get unowned partitions", e);
        }
    }

    @Override
    public List<PartitionState> getPartitionsOwnedBy(String consumerId) {
        try {
            String sql = "SELECT partition_id, owner_id, last_checkpoint, version_epoch, last_heartbeat " +
                    "FROM pxrs_partitions WHERE owner_id = ?";
            List<PartitionState> result = new ArrayList<>();
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, consumerId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.add(mapPartitionState(rs));
                    }
                }
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get partitions owned by: " + consumerId, e);
        }
    }

    @Override
    public boolean claimPartition(int partitionId, String consumerId, long expectedEpoch) {
        try {
            String sql = "UPDATE pxrs_partitions SET owner_id = ?, version_epoch = ? + 1, last_heartbeat = ? " +
                    "WHERE partition_id = ? AND version_epoch = ? AND (owner_id = '' OR owner_id IS NULL)";
            long now = System.currentTimeMillis();
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, consumerId);
                ps.setLong(2, expectedEpoch);
                ps.setLong(3, now);
                ps.setInt(4, partitionId);
                ps.setLong(5, expectedEpoch);
                return ps.executeUpdate() == 1;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to claim partition: " + partitionId, e);
        }
    }

    @Override
    public void releasePartition(int partitionId, String consumerId) {
        try {
            String sql = "UPDATE pxrs_partitions SET owner_id = '', version_epoch = version_epoch + 1, last_heartbeat = 0 " +
                    "WHERE partition_id = ? AND owner_id = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setInt(1, partitionId);
                ps.setString(2, consumerId);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to release partition: " + partitionId, e);
        }
    }

    @Override
    public void releaseAllPartitions(String consumerId) {
        try {
            String sql = "UPDATE pxrs_partitions SET owner_id = '', version_epoch = version_epoch + 1, last_heartbeat = 0 " +
                    "WHERE owner_id = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, consumerId);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to release all partitions for: " + consumerId, e);
        }
    }

    @Override
    public boolean updateCheckpoint(int partitionId, String consumerId, long checkpoint, long expectedEpoch) {
        try {
            String sql = "UPDATE pxrs_partitions SET last_checkpoint = ?, last_heartbeat = ? " +
                    "WHERE partition_id = ? AND owner_id = ? AND version_epoch = ?";
            long now = System.currentTimeMillis();
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setLong(1, checkpoint);
                ps.setLong(2, now);
                ps.setInt(3, partitionId);
                ps.setString(4, consumerId);
                ps.setLong(5, expectedEpoch);
                return ps.executeUpdate() == 1;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update checkpoint for partition: " + partitionId, e);
        }
    }

    @Override
    public long getCheckpoint(int partitionId) {
        try {
            String sql = "SELECT last_checkpoint FROM pxrs_partitions WHERE partition_id = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setInt(1, partitionId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getLong("last_checkpoint");
                    }
                    return 0;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get checkpoint for partition: " + partitionId, e);
        }
    }

    @Override
    public List<PartitionState> getExpiredPartitions() {
        try {
            String sql = "SELECT p.partition_id, p.owner_id, p.last_checkpoint, p.version_epoch, p.last_heartbeat " +
                    "FROM pxrs_partitions p " +
                    "WHERE p.owner_id != '' AND p.owner_id IS NOT NULL AND (" +
                    "  p.owner_id NOT IN (SELECT consumer_id FROM pxrs_consumers) " +
                    "  OR (p.last_heartbeat > 0 AND ? - p.last_heartbeat > ?)" +
                    ")";
            long now = System.currentTimeMillis();
            List<PartitionState> result = new ArrayList<>();
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setLong(1, now);
                ps.setLong(2, heartbeatTimeoutMs);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.add(mapPartitionState(rs));
                    }
                }
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get expired partitions", e);
        }
    }

    public void updateHeartbeat(String consumerId, int partitionId) {
        try {
            long now = System.currentTimeMillis();
            String partitionSql = "UPDATE pxrs_partitions SET last_heartbeat = ? " +
                    "WHERE partition_id = ? AND owner_id = ?";
            try (PreparedStatement ps = connection.prepareStatement(partitionSql)) {
                ps.setLong(1, now);
                ps.setInt(2, partitionId);
                ps.setString(3, consumerId);
                ps.executeUpdate();
            }

            String consumerSql = "UPDATE pxrs_consumers SET last_seen = ? WHERE consumer_id = ?";
            try (PreparedStatement ps = connection.prepareStatement(consumerSql)) {
                ps.setLong(1, now);
                ps.setString(2, consumerId);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update heartbeat for: " + consumerId, e);
        }
    }

    private PartitionState mapPartitionState(ResultSet rs) throws SQLException {
        return new PartitionState(
                rs.getInt("partition_id"),
                rs.getString("owner_id"),
                rs.getLong("last_checkpoint"),
                rs.getLong("version_epoch"),
                rs.getLong("last_heartbeat"));
    }
}
