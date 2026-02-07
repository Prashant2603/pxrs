package com.pxrs.store;

import com.pxrs.config.PxrsConfig;
import com.pxrs.model.ConsumerInfo;
import com.pxrs.model.PartitionState;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EtcdRegistryStore implements RegistryStore {

    private final PxrsConfig config;
    private Client client;
    private KV kvClient;
    private Lease leaseClient;
    private long leaseId;

    public EtcdRegistryStore(PxrsConfig config) {
        this.config = config;
    }

    private ByteSequence bs(String s) {
        return ByteSequence.from(s, StandardCharsets.UTF_8);
    }

    private String str(ByteSequence bs) {
        return bs.toString(StandardCharsets.UTF_8);
    }

    private String partitionKey(int partitionId) {
        return config.getKeyPrefix() + "partitions/" + partitionId + "/state";
    }

    private String consumerKey(String consumerId) {
        return config.getKeyPrefix() + "consumers/" + consumerId;
    }

    private String encodePartitionState(PartitionState ps) {
        return ps.getOwnerId() + "|" + ps.getLastCheckpoint() + "|" +
                ps.getVersionEpoch() + "|" + ps.getLastHeartbeat();
    }

    private PartitionState decodePartitionState(int partitionId, String value) {
        String[] parts = value.split("\\|", -1);
        if (parts.length < 4) {
            return new PartitionState(partitionId);
        }
        String ownerId = parts[0];
        long lastCheckpoint = Long.parseLong(parts[1]);
        long versionEpoch = Long.parseLong(parts[2]);
        long lastHeartbeat = Long.parseLong(parts[3]);
        return new PartitionState(partitionId, ownerId, lastCheckpoint, versionEpoch, lastHeartbeat);
    }

    @Override
    public void initialize(int numPartitions) {
        client = Client.builder()
                .endpoints(config.getEtcdEndpoints().split(","))
                .build();
        kvClient = client.getKVClient();
        leaseClient = client.getLeaseClient();

        try {
            LeaseGrantResponse leaseGrant = leaseClient
                    .grant(config.getLeaseTtlSeconds())
                    .get(5, TimeUnit.SECONDS);
            leaseId = leaseGrant.getID();
            leaseClient.keepAlive(leaseId, new io.grpc.stub.StreamObserver<io.etcd.jetcd.lease.LeaseKeepAliveResponse>() {
                @Override
                public void onNext(io.etcd.jetcd.lease.LeaseKeepAliveResponse response) {
                }

                @Override
                public void onError(Throwable throwable) {
                    System.err.println("[EtcdRegistryStore] Lease keepAlive error: " + throwable.getMessage());
                }

                @Override
                public void onCompleted() {
                }
            });
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to create etcd lease", e);
        }

        for (int i = 0; i < numPartitions; i++) {
            String key = partitionKey(i);
            try {
                GetResponse resp = kvClient.get(bs(key)).get(5, TimeUnit.SECONDS);
                if (resp.getKvs().isEmpty()) {
                    PartitionState ps = new PartitionState(i);
                    kvClient.put(bs(key), bs(encodePartitionState(ps)))
                            .get(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException("Failed to initialize partition " + i, e);
            }
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void registerConsumer(String consumerId) {
        try {
            PutOption option = PutOption.newBuilder()
                    .withLeaseId(leaseId)
                    .build();
            String value = System.currentTimeMillis() + "";
            kvClient.put(bs(consumerKey(consumerId)), bs(value), option)
                    .get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to register consumer " + consumerId, e);
        }
    }

    @Override
    public void deregisterConsumer(String consumerId) {
        try {
            kvClient.delete(bs(consumerKey(consumerId)))
                    .get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to deregister consumer " + consumerId, e);
        }
    }

    @Override
    public List<ConsumerInfo> getActiveConsumers() {
        try {
            String prefix = config.getKeyPrefix() + "consumers/";
            GetOption option = GetOption.newBuilder()
                    .isPrefix(true)
                    .build();
            GetResponse resp = kvClient.get(bs(prefix), option)
                    .get(5, TimeUnit.SECONDS);
            List<ConsumerInfo> result = new ArrayList<>();
            for (KeyValue kv : resp.getKvs()) {
                String key = str(kv.getKey());
                String consumerId = key.substring(prefix.length());
                String value = str(kv.getValue());
                long registeredAt = Long.parseLong(value);
                result.add(new ConsumerInfo(consumerId, registeredAt, System.currentTimeMillis()));
            }
            return result;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to get active consumers", e);
        }
    }

    @Override
    public PartitionState getPartitionState(int partitionId) {
        try {
            GetResponse resp = kvClient.get(bs(partitionKey(partitionId)))
                    .get(5, TimeUnit.SECONDS);
            if (resp.getKvs().isEmpty()) {
                return new PartitionState(partitionId);
            }
            return decodePartitionState(partitionId, str(resp.getKvs().get(0).getValue()));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to get partition state " + partitionId, e);
        }
    }

    @Override
    public List<PartitionState> getAllPartitionStates() {
        try {
            String prefix = config.getKeyPrefix() + "partitions/";
            GetOption option = GetOption.newBuilder()
                    .isPrefix(true)
                    .build();
            GetResponse resp = kvClient.get(bs(prefix), option)
                    .get(5, TimeUnit.SECONDS);
            List<PartitionState> result = new ArrayList<>();
            for (KeyValue kv : resp.getKvs()) {
                String key = str(kv.getKey());
                String idStr = key.replace(prefix, "").replace("/state", "");
                int partitionId = Integer.parseInt(idStr);
                result.add(decodePartitionState(partitionId, str(kv.getValue())));
            }
            return result;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to get all partition states", e);
        }
    }

    @Override
    public List<PartitionState> getUnownedPartitions() {
        List<PartitionState> all = getAllPartitionStates();
        List<PartitionState> unowned = new ArrayList<>();
        for (PartitionState ps : all) {
            if (ps.getOwnerId() == null || ps.getOwnerId().isEmpty()) {
                unowned.add(ps);
            }
        }
        return unowned;
    }

    @Override
    public List<PartitionState> getPartitionsOwnedBy(String consumerId) {
        List<PartitionState> all = getAllPartitionStates();
        List<PartitionState> owned = new ArrayList<>();
        for (PartitionState ps : all) {
            if (consumerId.equals(ps.getOwnerId())) {
                owned.add(ps);
            }
        }
        return owned;
    }

    @Override
    public boolean claimPartition(int partitionId, String consumerId, long expectedEpoch) {
        try {
            String key = partitionKey(partitionId);
            GetResponse current = kvClient.get(bs(key)).get(5, TimeUnit.SECONDS);
            if (current.getKvs().isEmpty()) {
                return false;
            }

            KeyValue kv = current.getKvs().get(0);
            PartitionState ps = decodePartitionState(partitionId, str(kv.getValue()));
            if (ps.getVersionEpoch() != expectedEpoch) {
                return false;
            }
            if (ps.getOwnerId() != null && !ps.getOwnerId().isEmpty()) {
                return false;
            }

            PartitionState newState = new PartitionState(
                    partitionId, consumerId, ps.getLastCheckpoint(),
                    expectedEpoch + 1, System.currentTimeMillis());

            Cmp cmp = new Cmp(bs(key), Cmp.Op.EQUAL,
                    CmpTarget.modRevision(kv.getModRevision()));

            TxnResponse txnResp = kvClient.txn()
                    .If(cmp)
                    .Then(Op.put(bs(key), bs(encodePartitionState(newState)), PutOption.DEFAULT))
                    .commit()
                    .get(5, TimeUnit.SECONDS);

            return txnResp.isSucceeded();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to claim partition " + partitionId, e);
        }
    }

    @Override
    public void releasePartition(int partitionId, String consumerId) {
        try {
            String key = partitionKey(partitionId);
            GetResponse current = kvClient.get(bs(key)).get(5, TimeUnit.SECONDS);
            if (current.getKvs().isEmpty()) {
                return;
            }
            KeyValue kv = current.getKvs().get(0);
            PartitionState ps = decodePartitionState(partitionId, str(kv.getValue()));
            if (!consumerId.equals(ps.getOwnerId())) {
                return;
            }
            PartitionState newState = new PartitionState(
                    partitionId, "", ps.getLastCheckpoint(),
                    ps.getVersionEpoch() + 1, 0);
            kvClient.put(bs(key), bs(encodePartitionState(newState)))
                    .get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to release partition " + partitionId, e);
        }
    }

    @Override
    public void releaseAllPartitions(String consumerId) {
        List<PartitionState> owned = getPartitionsOwnedBy(consumerId);
        for (PartitionState ps : owned) {
            releasePartition(ps.getPartitionId(), consumerId);
        }
    }

    @Override
    public boolean updateCheckpoint(int partitionId, String consumerId,
                                    long checkpoint, long expectedEpoch) {
        try {
            String key = partitionKey(partitionId);
            GetResponse current = kvClient.get(bs(key)).get(5, TimeUnit.SECONDS);
            if (current.getKvs().isEmpty()) {
                return false;
            }
            KeyValue kv = current.getKvs().get(0);
            PartitionState ps = decodePartitionState(partitionId, str(kv.getValue()));
            if (ps.getVersionEpoch() != expectedEpoch) {
                return false;
            }
            if (!consumerId.equals(ps.getOwnerId())) {
                return false;
            }
            PartitionState newState = new PartitionState(
                    partitionId, consumerId, checkpoint,
                    expectedEpoch, System.currentTimeMillis());

            Cmp cmp = new Cmp(bs(key), Cmp.Op.EQUAL,
                    CmpTarget.modRevision(kv.getModRevision()));

            TxnResponse txnResp = kvClient.txn()
                    .If(cmp)
                    .Then(Op.put(bs(key), bs(encodePartitionState(newState)), PutOption.DEFAULT))
                    .commit()
                    .get(5, TimeUnit.SECONDS);

            return txnResp.isSucceeded();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to update checkpoint for partition " + partitionId, e);
        }
    }

    @Override
    public long getCheckpoint(int partitionId) {
        return getPartitionState(partitionId).getLastCheckpoint();
    }

    @Override
    public List<PartitionState> getExpiredPartitions() {
        List<PartitionState> all = getAllPartitionStates();
        Set<String> activeConsumerIds = new HashSet<>();
        for (ConsumerInfo ci : getActiveConsumers()) {
            activeConsumerIds.add(ci.getConsumerId());
        }
        List<PartitionState> expired = new ArrayList<>();
        for (PartitionState ps : all) {
            if (ps.getOwnerId() != null && !ps.getOwnerId().isEmpty()) {
                if (!activeConsumerIds.contains(ps.getOwnerId())) {
                    expired.add(ps);
                }
            }
        }
        return expired;
    }
}
