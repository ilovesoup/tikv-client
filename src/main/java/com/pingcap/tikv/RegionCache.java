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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.grpc.Metapb.Region;

import java.util.concurrent.Future;


public class RegionCache {
    private final ReadOnlyPDClient pdClient;
    private final LoadingCache<ByteString, Future<Region>> cache;

    public static final int MAX_CACHE_CAPACITY = 4096;

    // To avoid double retrieval, we wrap the rpc results in a future object
    // When rpc not returned, instead of call again, it wait for previous one done
    private RegionCache(ReadOnlyPDClient pdClient) {
        this.pdClient = pdClient;
        cache = CacheBuilder.newBuilder()
                .maximumSize(MAX_CACHE_CAPACITY)
                .build(new CacheLoader<ByteString, Future<Region>>() {
                    public Future load(ByteString key) {
                        SettableFuture<Region> f = SettableFuture.create();
                        f.set(pdClient.getRegionByKey(key));
                        return f;
                    }
                });
    }

    public Region getRegionByKey(ByteString key) {
        try {
            return cache.getUnchecked(key).get();
        } catch (Exception e) {
            throw new GrpcException(e);
        }
    }
}
