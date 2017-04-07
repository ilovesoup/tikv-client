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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.grpc.Metapb;
import com.pingcap.tikv.grpc.PDGrpc;
import com.pingcap.tikv.grpc.Pdpb.*;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.policy.RetryNTimes;
import com.pingcap.tikv.policy.RetryPolicy;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class PDClient implements AutoCloseable, ReadOnlyPDClient {
    private static final Logger         logger = LogManager.getFormatterLogger(PDClient.class);
    private List<HostAndPort>           pdUrls;
    private RequestHeader               header;
    private TsoRequest                  tsoReq;
    private volatile LeaderWrapper      leaderWrapper;
    private ScheduledExecutorService    service;
    private RetryPolicy.Builder         retryPolicyBuilder;
    private int                         timeout;
    private TimeUnit                    timeoutUnit;
    private ReentrantLock               leaderUpdateLock = new ReentrantLock();


    @Override
    public TiTimestamp getTimestamp() {
        return callWithRetry(() -> getTimestampHelper(), "getTimestamp");
    }

    @Override
    public Region getRegionByKey(ByteString key) {
        GetRegionRequest request = GetRegionRequest.newBuilder()
                .setHeader(header)
                .setRegionKey(key)
                .build();

        GetRegionResponse resp = callWithRetry(() -> getBlockingStub().getRegion(request), "getRegionByKey");
        return resp.getRegion();
    }

    @Override
    public Region getRegionByID(long id) {
        GetRegionByIDRequest request = GetRegionByIDRequest.newBuilder()
                .setHeader(header)
                .setRegionId(id)
                .build();

        GetRegionResponse resp = callWithRetry(() -> getBlockingStub().getRegionByID(request), "getRegionByID");
        // Instead of using default leader instance, explicitly set no leader to null
        return resp.getRegion();
    }

    @Override
    public Store getStore(long storeId) {
        GetStoreRequest request = GetStoreRequest.newBuilder()
                .setHeader(header)
                .setStoreId(storeId)
                .build();

        GetStoreResponse resp = callWithRetry(() -> getBlockingStub().getStore(request), "getStore");
        Store store = resp.getStore();
        if (store.getState() == Metapb.StoreState.Tombstone) {
            return null;
        }
        return store;
    }

    @Override
    public void close() throws InterruptedException {
        if (service != null) {
            service.shutdownNow();
        }
        if (getLeaderWrapper() != null) {
            getLeaderWrapper().close();
        }
    }

    private PDClient(int timeout, TimeUnit timeoutUnit, RetryPolicy.Builder retryPolicyBuilder) {
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        this.retryPolicyBuilder = retryPolicyBuilder;
    }

    private <T> T callWithRetry(Callable<T> proc, String methodName) {
        return retryPolicyBuilder.create(() -> { updateLeader(getMembers()); return null; })
                .callWithRetry(proc, methodName);
    }

    private TiTimestamp getTimestampHelper() throws ExecutionException, InterruptedException {
        SettableFuture<com.pingcap.tikv.grpc.Pdpb.Timestamp> resultFuture = SettableFuture.create();
        StreamObserver<TsoResponse> responseObserver = new StreamObserver<TsoResponse>() {
            private SettableFuture<com.pingcap.tikv.grpc.Pdpb.Timestamp> resultFuture;

            public StreamObserver<TsoResponse> bind(SettableFuture<com.pingcap.tikv.grpc.Pdpb.Timestamp> resultFuture) {
                this.resultFuture = resultFuture;
                return this;
            }

            @Override
            public void onNext(TsoResponse value) {
                resultFuture.set(value.getTimestamp());
            }

            @Override
            public void onError(Throwable t) {
                resultFuture.setException(t);
            }

            @Override
            public void onCompleted() {}

        }.bind(resultFuture);
        StreamObserver<TsoRequest> requestObserver = getAsyncStub().tso(responseObserver);
        requestObserver.onNext(tsoReq);
        requestObserver.onCompleted();
        com.pingcap.tikv.grpc.Pdpb.Timestamp resp = resultFuture.get();
        return new TiTimestamp(resp.getPhysical(), resp.getLogical());
    }

    @VisibleForTesting
    RequestHeader getHeader() {
        return header;
    }

    @VisibleForTesting
    LeaderWrapper getLeaderWrapper() {
        return leaderWrapper;
    }

    class LeaderWrapper {
        private final HostAndPort           leaderInfo;
        private final PDGrpc.PDBlockingStub blockingStub;
        private final PDGrpc.PDStub         asyncStub;
        private final ManagedChannel        channel;
        private final long                  createTime;

        public LeaderWrapper(HostAndPort leaderInfo,
                             PDGrpc.PDBlockingStub blockingStub,
                             PDGrpc.PDStub asyncStub,
                             ManagedChannel channel,
                             long createTime) {
            this.leaderInfo = leaderInfo;
            this.blockingStub = blockingStub;
            this.asyncStub = asyncStub;
            this.channel = channel;
            this.createTime = createTime;
        }

        public HostAndPort getLeaderInfo() {
            return leaderInfo;
        }

        public PDGrpc.PDBlockingStub getBlockingStub() {
            return blockingStub;
        }

        public PDGrpc.PDStub getAsyncStub() {
            return asyncStub;
        }

        public long getCreateTime() {
            return createTime;
        }

        public void close() {
            channel.isShutdown();
        }
    }

    private ManagedChannel getManagedChannel(HostAndPort url) {
        return ManagedChannelBuilder
                .forAddress(url.getHostText(), url.getPort())
                .usePlaintext(true)
                .build();
    }

    private GetMembersResponse getMembers() {
        for (HostAndPort url : pdUrls) {
            ManagedChannel probChan = null;
            try {
                probChan = getManagedChannel(url);
                PDGrpc.PDBlockingStub stub = PDGrpc.newBlockingStub(probChan);
                GetMembersRequest request = GetMembersRequest.newBuilder()
                        .setHeader(RequestHeader.getDefaultInstance())
                        .build();
                return stub.getMembers(request);
            }
            catch (Exception ignore) {}
            finally {
                if (probChan != null) {
                    probChan.shutdownNow();
                }
            }
        }
        return null;
    }

    private void updateLeader(GetMembersResponse resp) {
        String leaderUrlStr = "URL Not Set";
        try {
            long ts = System.nanoTime();
            leaderUpdateLock.lock();
            // Lock for not flooding during pd error
            if (leaderWrapper != null && leaderWrapper.getCreateTime() > ts) return;

            if (resp == null) {
                resp = getMembers();
                if (resp == null) return;
            }
            Member leader = resp.getLeader();
            List<String> leaderUrls = leader.getClientUrlsList();
            if (leaderUrls.isEmpty()) return;
            leaderUrlStr = leaderUrls.get(0);
            // TODO: Why not strip protocol info on server side since grpc does not need it
            URL tURL = new URL(leaderUrlStr);
            HostAndPort newLeader = HostAndPort.fromParts(tURL.getHost(), tURL.getPort());
            if (leaderWrapper != null && newLeader.equals(leaderWrapper.getLeaderInfo())) {
                return;
            }

            // switch leader
            ManagedChannel clientChannel = getManagedChannel(newLeader);
            leaderWrapper = new LeaderWrapper(newLeader,
                    PDGrpc.newBlockingStub(clientChannel),
                    PDGrpc.newStub(clientChannel),
                    clientChannel,
                    System.nanoTime());
            logger.info("Switched to new leader " + newLeader.toString());
        } catch (MalformedURLException e) {
            logger.error("Client URL is not valid: " + leaderUrlStr, e);
        } catch (Exception e) {
            logger.error("Error updating leader.", e);
        } finally {
            leaderUpdateLock.unlock();
        }
    }

    private PDGrpc.PDBlockingStub getBlockingStub() {
        return leaderWrapper.getBlockingStub()
                            .withDeadlineAfter(timeout, timeoutUnit);
    }

    private PDGrpc.PDStub getAsyncStub() {
        return leaderWrapper.getAsyncStub()
                            .withDeadlineAfter(timeout, timeoutUnit);
    }

    private void initCluster(List<HostAndPort> urls) {
        pdUrls = urls;
        GetMembersResponse resp = getMembers();
        checkNotNull(resp, "Failed to init client for PD cluster.");
        long clusterId = resp.getHeader().getClusterId();
        header = RequestHeader.newBuilder().setClusterId(clusterId).build();
        tsoReq = TsoRequest.newBuilder().setHeader(header).build();
        updateLeader(resp);
        service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(new LeaderCheckTask(), 1, 1, TimeUnit.MINUTES);
    }

    private class LeaderCheckTask implements Runnable {
        @Override
        public void run() {
            updateLeader(null);
        }
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
        private List<String> pdAddrs = new ArrayList<>();

        public Builder setTimeout(int timeout, TimeUnit unit) {
            this.timeout = timeout;
            this.timeoutUnit = unit;
            return this;
        }

        public Builder setRetryPolicyBuilder(RetryPolicy.Builder builder) {
            this.builder = builder;
            return this;
        }

        public Builder addAddress(String address) {
            pdAddrs.add(address);
            return this;
        }

        public ReadOnlyPDClient build() {
            return buildHelper();
        }

        @VisibleForTesting
        PDClient buildRaw() {
            return buildHelper();
        }

        private PDClient buildHelper() {
            checkArgument(pdAddrs.size() > 0, "No PD address specified.");

            List<HostAndPort> urls = ImmutableList.copyOf(Iterables.transform(ImmutableSet.copyOf(pdAddrs).asList(),
                                                                              addStr -> HostAndPort.fromString(addStr)));
            PDClient client = null;
            try {
                client = new PDClient(timeout, timeoutUnit, builder);
                client.initCluster(urls);
            } catch (Exception e) {
                if (client != null) {
                    try {
                        client.close();
                    } catch (InterruptedException ignore) {
                    }
                }
            }
            return client;
        }

        private Builder() {}
    }
}