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
import com.pingcap.tikv.util.FutureObserver;
import com.pingcap.tikv.util.VoidCallable;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class RegionStoreClient implements AutoCloseable {
    private static final Logger             logger = LogManager.getFormatterLogger(RegionStoreClient.class);
    private final TiKVGrpc.TiKVBlockingStub blockingStub;
    private final TiKVGrpc.TiKVStub         asyncStub;
    private final ManagedChannel            channel;
    private Context                         context;
    private TiSession                       session;
    private TiConfiguration                 conf;

    private RegionStoreClient(Region region, TiSession session,
                              ManagedChannel channel,
                              TiKVGrpc.TiKVBlockingStub blockingStub,
                              TiKVGrpc.TiKVStub   asyncStub) {
        this.session = session;
        this.conf = session.getConf();
        this.channel = channel;
        this.blockingStub = blockingStub;
        this.asyncStub = asyncStub;
        this.context = Context.newBuilder()
                              .setRegionId(region.getId())
                              .setRegionEpoch(region.getRegionEpoch())
                              .setPeer(region.getPeers(0))
                              .build();
    }

    private TiKVGrpc.TiKVBlockingStub getBlockingStub() {
        return blockingStub.withDeadlineAfter(getConf().getTimeout(),
                                              getConf().getTimeoutUnit());
    }

    private TiKVGrpc.TiKVStub getAsyncStub() {
        return asyncStub.withDeadlineAfter(getConf().getTimeout(),
                                           getConf().getTimeoutUnit());
    }

    private <T> T callWithRetry(Callable<T> proc, String methodName) {
        return getSession()
                .getRetryPolicyBuilder()
                .create(null).callWithRetry(proc, methodName);
    }

    private void callAsyncWithRetry(VoidCallable proc, String methodName) {
        getSession()
                .getRetryPolicyBuilder()
                .create(null).callWithRetry(() -> { proc.call(); return null; }, methodName);
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

    public Future<ByteString> getAsync(ByteString key, long version) {
        FutureObserver<ByteString, GetResponse> responseObserver =
                new FutureObserver<>((GetResponse resp) -> resp.getValue());
        GetRequest request = GetRequest.newBuilder()
                .setContext(context)
                .setKey(key)
                .setVersion(version)
                .build();

        callAsyncWithRetry(() -> getAsyncStub().kvGet(request, responseObserver), "getAsync");
        return responseObserver.getFuture();
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

    public Future<List<KvPair>> batchGetAsync(Iterable<ByteString> keys, long version) {
        FutureObserver<List<KvPair>, BatchGetResponse> responseObserver =
                new FutureObserver<>((BatchGetResponse resp) -> resp.getPairsList());

        BatchGetRequest request = BatchGetRequest.newBuilder()
                .setContext(context)
                .addAllKeys(keys)
                .setVersion(version)
                .build();

        callAsyncWithRetry(() -> getAsyncStub().kvBatchGet(request, responseObserver), "batchGetAsync");
        return responseObserver.getFuture();
    }

    public List<KvPair> scan(ByteString startKey, long version) {
        return scan(startKey, version, false);
    }

    public Future<List<KvPair>> scanAsync(ByteString startKey, long version) {
        return scanAsync(startKey, version, false);
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

    public Future<List<KvPair>> scanAsync(ByteString startKey, long version, boolean keyOnly) {
        FutureObserver<List<KvPair>, ScanResponse> responseObserver =
                new FutureObserver<>((ScanResponse resp) -> resp.getPairsList());

        ScanRequest request = ScanRequest.newBuilder()
                .setContext(context)
                .setStartKey(startKey)
                .setVersion(version)
                .setKeyOnly(keyOnly)
                .build();

        callAsyncWithRetry(() -> getAsyncStub().kvScan(request, responseObserver), "scanAsync");
        return responseObserver.getFuture();
    }

    @Override
    public void close() throws Exception {
        channel.shutdown();
    }

    public TiSession getSession() {
        return session;
    }

    public TiConfiguration getConf() {
        return conf;
    }

    public static RegionStoreClient create(Region region, Store store, TiSession session) {
        RegionStoreClient client = null;
        try {
            HostAndPort address = HostAndPort.fromString(store.getAddress());
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(address.getHostText(), address.getPort())
                    .usePlaintext(true)
                    .build();
            TiKVGrpc.TiKVBlockingStub blockingStub = TiKVGrpc.newBlockingStub(channel);
            TiKVGrpc.TiKVStub asyncStub = TiKVGrpc.newStub(channel);
            client = new RegionStoreClient(region, session, channel, blockingStub, asyncStub);
        } catch (Exception e) {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception ignore) {
                }
            }
        }
        return client;
    }
}
