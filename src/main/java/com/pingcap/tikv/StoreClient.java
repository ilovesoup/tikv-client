/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv;


import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.grpc.Kvrpcpb.Context;
import com.pingcap.tikv.grpc.Kvrpcpb.GetRequest;
import com.pingcap.tikv.grpc.Kvrpcpb.GetResponse;
import com.pingcap.tikv.grpc.Kvrpcpb.BatchGetResponse;
import com.pingcap.tikv.grpc.Kvrpcpb.BatchGetRequest;
import com.pingcap.tikv.grpc.Kvrpcpb.ScanResponse;
import com.pingcap.tikv.grpc.Kvrpcpb.ScanRequest;
import com.pingcap.tikv.grpc.Kvrpcpb.KvPair;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.grpc.TiKVGrpc;
import com.pingcap.tikv.policy.RetryNTimes;
import com.pingcap.tikv.policy.RetryPolicy;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class StoreClient implements AutoCloseable {
    private static final Logger             logger = LogManager.getFormatterLogger(StoreClient.class);
    private final TiKVGrpc.TiKVBlockingStub blockingStub;
    private final ManagedChannel            channel;
    private RetryPolicy.Builder             retryPolicyBuilder;
    private int                             timeout;
    private TimeUnit                        timeoutUnit;
    private Context                         context;

    private StoreClient(Region region, int timeout, TimeUnit timeoutUnit,
                        RetryPolicy.Builder retryPolicyBuilder,
                        ManagedChannel channel, TiKVGrpc.TiKVBlockingStub blockingStub) {
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        this.retryPolicyBuilder = retryPolicyBuilder;
        this.channel = channel;
        this.blockingStub = blockingStub;
        this.context = Context.newBuilder()
                              .setRegionId(region.getId())
                              .setRegionEpoch(region.getRegionEpoch())
                              .setPeer(region.getPeers(0))
                              .build();
    }

    private TiKVGrpc.TiKVBlockingStub getBlockingStub() {
        return blockingStub
                .withDeadlineAfter(timeout, timeoutUnit);
    }

    private <T> T callWithRetry(Callable<T> proc, String methodName) {
        return retryPolicyBuilder.create(null).callWithRetry(proc, methodName);
    }

    public ByteString get(ByteString key, long version) {
        GetRequest request = GetRequest.newBuilder()
                .setContext(context)
                .setKey(key)
                .setVersion(version)
                .build();
        GetResponse resp = callWithRetry(() -> getBlockingStub().kvGet(request), "kvGet");
        return resp.getValue();
    }

    public List<KvPair> batchGet(Iterable<ByteString> keys, long version) {
        BatchGetRequest request = BatchGetRequest.newBuilder()
                .setContext(context)
                .addAllKeys(keys)
                .setVersion(version)
                .build();
        BatchGetResponse resp = callWithRetry(() -> getBlockingStub().kvBatchGet(request), "kvBatchGet");
        return resp.getPairsList();
    }

    public List<KvPair> scan(ByteString startKey, long version) {
        return scan(startKey, version, false);
    }

    public List<KvPair> scan(ByteString startKey, long version, boolean keyOnly) {
        ScanRequest request = ScanRequest.newBuilder()
                .setContext(context)
                .setStartKey(startKey)
                .setVersion(version)
                .setKeyOnly(keyOnly)
                .build();
        ScanResponse resp = callWithRetry(() -> getBlockingStub().kvScan(request), "kvScan");
        return resp.getPairsList();
    }

    @Override
    public void close() throws Exception {
        channel.shutdown();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        public static final int DEF_TIMEOUT = 3;
        public static final TimeUnit DEF_TIMEOUT_UNIT = TimeUnit.SECONDS;
        public static final RetryPolicy.Builder DEF_RETRY_POLICY_BUILDER = new RetryNTimes.Builder(3);

        private RetryPolicy.Builder builder = DEF_RETRY_POLICY_BUILDER;
        private int timeout = DEF_TIMEOUT;
        private TimeUnit timeoutUnit = DEF_TIMEOUT_UNIT;

        public Builder setTimeout(int timeout, TimeUnit unit) {
            this.timeout = timeout;
            this.timeoutUnit = unit;
            return this;
        }

        public Builder setRetryPolicyBuilder(RetryPolicy.Builder builder) {
            this.builder = builder;
            return this;
        }

        public StoreClient build(Region region, Store store) {
            StoreClient client = null;
            try {
                HostAndPort address = HostAndPort.fromString(store.getAddress());
                ManagedChannel channel = ManagedChannelBuilder
                        .forAddress(address.getHostText(), address.getPort())
                        .usePlaintext(true)
                        .build();
                TiKVGrpc.TiKVBlockingStub blockingStub = TiKVGrpc.newBlockingStub(channel);
                client = new StoreClient(region, timeout, timeoutUnit, builder, channel, blockingStub);
            } catch (Exception e) {
                if (client != null) {
                    try {
                        client.close();
                    } catch (Exception ignore) {}
                }
            }
            return client;
        }

        private Builder() {}
    }
}
