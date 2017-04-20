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
import com.pingcap.tikv.codec.KeyUtils;
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
    private final static int EPOCH_SHIFT_BITS = 18;
    private final TiConfiguration conf;

    public Snapshot(Version version, RegionManager regionCache, TiSession session) {
        this.version = version;
        this.regionCache = regionCache;
        this.session = session;
        this.conf = session.getConf();
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
        private List<KvPair>    currentCache;
        private ByteString      startKey;
        private int             index = -1;
        private boolean         eof = false;

        public ScanIterator(ByteString startKey) {
            this.startKey = startKey;
        }

        private boolean loadCache() {
            if (eof) return false;

            Pair<Region, Store> pair = regionCache.getRegionStorePairByKey(startKey);
            Region region = pair.first;
            Store store = pair.second;
            try (RegionStoreClient client =
                         RegionStoreClient.create(region, store, getSession())) {
                currentCache = client.scan(startKey, version.getVersion());
                if (currentCache == null || currentCache.size() == 0) {
                    return false;
                }
                index = 0;
                // Session should be single-threaded itself
                // so that we don't worry about conf change in the middle
                // of a transaction. Otherwise below code might lose data
                if (currentCache.size() < conf.getScanBatchSize()) {
                    // Current region done, start new batch from next region
                    startKey = region.getEndKey();
                    if (startKey.size() == 0) {
                        eof = true;
                    }
                } else {
                    // Start new scan from exact next key in current region
                    ByteString lastKey = currentCache
                            .get(currentCache.size() - 1)
                            .getKey();
                    startKey = KeyUtils.getNextKeyInByteOrder(lastKey);
                }
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
            long t = System.currentTimeMillis() << EPOCH_SHIFT_BITS;
            return new Version(t);
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
