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

import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.Callable;

import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;

public abstract class AbstractGrpcClient<BlockingStubT extends AbstractStub<BlockingStubT>,
                                         StubT extends AbstractStub<StubT>> implements AutoCloseable {
    private TiSession          session;
    private TiConfiguration    conf;

    protected AbstractGrpcClient(TiSession session) {
        this.session = session;
        this.conf = session.getConf();
    }

    public TiSession getSession() {
        return session;
    }

    public TiConfiguration getConf() {
        return conf;
    }

    protected <ReqT, ResT> ResT callWithRetry(MethodDescriptor<ReqT, ResT> method,
                                              ReqT request) {
        return getSession()
                .getRetryPolicyBuilder()
                .create(getRecoveryMethod()).callWithRetry(() -> {
                            BlockingStubT stub = getBlockingStub();
                            return ClientCalls.blockingUnaryCall(
                                    stub.getChannel(), method, stub.getCallOptions(), request);
                        },
                        method.getFullMethodName());
    }

    protected <ReqT, ResT> void callAsyncWithRetry(MethodDescriptor<ReqT, ResT> method,
                                                 ReqT request,
                                                 StreamObserver<ResT> responseObserver) {
        getSession()
                .getRetryPolicyBuilder()
                .create(getRecoveryMethod())
                .callWithRetry(() -> {
                            StubT stub = getAsyncStub();
                            ClientCalls.asyncUnaryCall(
                                    stub.getChannel().newCall(method, stub.getCallOptions()),
                                    request, responseObserver);
                            return null;
                        },
                        method.getFullMethodName());
    }

    protected <ReqT, ResT> StreamObserver<ReqT>
    callBidiStreamingWithRetry(MethodDescriptor<ReqT, ResT> method,
                               StreamObserver<ResT> responseObserver) {
        return getSession()
                .getRetryPolicyBuilder()
                .create(getRecoveryMethod())
                .callWithRetry(() -> {
                            StubT stub = getAsyncStub();
                            return asyncBidiStreamingCall(
                                        stub.getChannel().newCall(method, stub.getCallOptions()), responseObserver);
                        },
                        method.getFullMethodName());
    }

    protected abstract BlockingStubT getBlockingStub();
    protected abstract StubT getAsyncStub();

    // TODO: A little bit odd to put here, should be inside policy
    protected Callable<Void> getRecoveryMethod() {
        return null;
    }
}
