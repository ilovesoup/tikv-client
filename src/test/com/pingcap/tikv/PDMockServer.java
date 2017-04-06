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

import com.google.common.base.Optional;
import com.pingcap.tikv.grpc.PDGrpc;
import com.pingcap.tikv.grpc.Pdpb;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;

import com.pingcap.tikv.grpc.Pdpb.*;

public class PDMockServer extends PDGrpc.PDImplBase {
    public int port;
    private long clusterId;

    public Server server;

    public void addGetMemberResp(GetMembersResponse r) {
        getMembersResp.addLast(Optional.fromNullable(r));
    }

    private final Deque<Optional<GetMembersResponse>> getMembersResp = new LinkedBlockingDeque<>();
    @Override
    public void getMembers(Pdpb.GetMembersRequest request,
                           StreamObserver<GetMembersResponse> resp) {
        try {
            resp.onNext(getMembersResp.removeFirst().get());
            resp.onCompleted();
        } catch (Exception e) {
            resp.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    @Override
    public StreamObserver<TsoRequest> tso(StreamObserver<TsoResponse> resp) {
        return new StreamObserver<TsoRequest>() {
            private int physical = 1;
            private int logical = 0;

            @Override
            public void onNext(Pdpb.TsoRequest value) {}

            @Override
            public void onError(Throwable t) {}

            @Override
            public void onCompleted() {
                resp.onNext(GrpcUtils.makeTsoResponse(clusterId, physical++, logical++));
                resp.onCompleted();
            }
        };
    }

    public void addGetRegionResp(GetRegionResponse r) {
        getRegionResp.addLast(r);
    }

    private final Deque<GetRegionResponse> getRegionResp = new LinkedBlockingDeque<>();

    @Override
    public void getRegion(GetRegionRequest request, StreamObserver<GetRegionResponse> resp) {
        try {
            resp.onNext(getRegionResp.removeFirst());
            resp.onCompleted();
        } catch (Exception e) {
            resp.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    public void addGetRegionByIDResp(GetRegionResponse r) {
        getRegionByIDResp.addLast(r);
    }

    private final Deque<GetRegionResponse> getRegionByIDResp = new LinkedBlockingDeque<>();
    @Override
    public void getRegionByID(GetRegionByIDRequest request, StreamObserver<GetRegionResponse> resp) {
        try {
            resp.onNext(getRegionByIDResp.removeFirst());
            resp.onCompleted();
        } catch (Exception e) {
            resp.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    public void addGetStoreResp(GetStoreResponse r) {
        getStoreResp.addLast(Optional.fromNullable(r));
    }
    private final Deque<Optional<GetStoreResponse>> getStoreResp = new LinkedBlockingDeque<>();
    public void getStore(GetStoreRequest request,
                         StreamObserver<GetStoreResponse> resp) {
        try {
            resp.onNext(getStoreResp.removeFirst().get());
            resp.onCompleted();
        } catch (Exception e) {
            resp.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    public int start(long clusterId) throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        this.clusterId = clusterId;
        server = ServerBuilder.forPort(port)
                .addService(this)
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> PDMockServer.this.stop()));
        return port;
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public long getClusterId() {
        return clusterId;
    }
}
