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


import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.grpc.Kvrpcpb.KvPair;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.util.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Snapshot {
    private final Version version;
    private final RegionManager regionCache;
    private final TiSession session;

    public Snapshot(Version version, RegionManager regionCache, TiSession session) {
        this.version = version;
        this.regionCache = regionCache;
        this.session = session;
    }

    public Snapshot(RegionManager regionCache, TiSession session) {
        this(Version.getCurrentTSAsVersion(), regionCache, session);
    }

    public TiSession getSession() {
        return session;
    }

    public byte[] get(byte[] key) {
        ByteString keyString = ByteString.copyFrom(key);
        ByteString value = get(keyString);
        return value.toByteArray();
    }

    public ByteString get(ByteString key) {
        Pair<Region, Store> pair = regionCache.getRegionStorePairByKey(key);
        RegionStoreClient client = RegionStoreClient
                .create(pair.first, pair.second, getSession());
        // TODO: Need to deal with lock error after grpc stable
        return client.get(key, version.getVersion());
    }

    private class ScanIterator implements Iterator<KvPair> {
        private List<KvPair> currentCache;
        private int index = -1;
        private ByteString startKey;

        public ScanIterator(ByteString startKey) {
            this.startKey = startKey;
        }

        private boolean loadCache() {
            Pair<Region, Store> pair = regionCache.getRegionStorePairByKey(startKey);
            try (RegionStoreClient client = RegionStoreClient
                    .create(pair.first, pair.second, getSession())) {
                //currentCache = client.scan(startKey, version.getVersion());
                currentCache = client.scan(pair.first.getStartKey(), version.getVersion());
                startKey = pair.first.getEndKey();
                if (currentCache == null || currentCache.size() == 0) {
                    return false;
                }
                index = 0;
            } catch (Exception e) {
                throw new TiClientInternalException("Error Closing Store client.", e);
            }
            return true;
        }

        @Override
        public boolean hasNext() {
            if (index == -1 || index >= currentCache.size()) {
                if (!loadCache()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public KvPair next() {
            if (index < currentCache.size()) {
                return currentCache.get(index++);
            }
            if (!loadCache()) {
                return null;
            }
            return currentCache.get(index++);
        }
    }

    public Iterator<KvPair> scan(ByteString startKey) {
        return new ScanIterator(startKey);
    }

    // TODO: Do we really need to transfer key again?
    // TODO: Need faster implementation, say concurrent version
    // Assume keys sorted
    public List<KvPair> batchGet(List<ByteString> keys) {
        Region curRegion = null;
        Range<ByteBuffer> curKeyRange = null;
        Pair<Region, Store> lastPair = null;
        List<ByteString> keyBuffer = new ArrayList<>();
        List<KvPair> result = new ArrayList<>(keys.size());
        for (ByteString key : keys) {
            if (curRegion == null || !curKeyRange.contains(key.asReadOnlyByteBuffer())) {
                Pair<Region, Store> pair = regionCache.getRegionStorePairByKey(key);
                lastPair = pair;
                curRegion = pair.first;
                curKeyRange = Range.closedOpen(curRegion.getStartKey().asReadOnlyByteBuffer(),
                                               curRegion.getEndKey().asReadOnlyByteBuffer());
                if (lastPair != null) {
                    try (RegionStoreClient client = RegionStoreClient
                            .create(lastPair.first, lastPair.second, getSession())) {
                        List<KvPair> partialResult = client.batchGet(keyBuffer, version.getVersion());
                        for (KvPair kv : partialResult) {
                            // TODO: Add lock check
                            result.add(kv);
                        }
                    } catch (Exception e) {
                        throw new TiClientInternalException("Error Closing Store client.", e);
                    }
                    keyBuffer = new ArrayList<>();
                }
                keyBuffer.add(key);
            }
        }
        return result;
    }

    public static class Version {
        public static Version getCurrentTSAsVersion() {
            long ts = System.nanoTime() / TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS);
            return new Version(ts);
        }

        private final long version;
        private Version(long ts) {
            version = ts;
        }

        public long getVersion() {
            return version;
        }
    }
}
