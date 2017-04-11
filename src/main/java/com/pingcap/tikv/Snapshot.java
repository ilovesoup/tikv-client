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


import com.google.protobuf.ByteString;
import com.pingcap.tikv.grpc.Kvrpcpb.KvPair;

import java.util.List;

public class Snapshot {
    private final long version;
    private final RegionManager regionCache;

    public Snapshot(long version, RegionManager regionCache) {
        this.version = version;
        this.regionCache = regionCache;
    }
}