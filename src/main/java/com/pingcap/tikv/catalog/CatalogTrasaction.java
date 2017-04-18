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

package com.pingcap.tikv.catalog;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.BytesUtils;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.util.IterableItertor;
import com.pingcap.tikv.util.Pair;

import java.util.List;


public class CatalogTrasaction {
    private final Snapshot snapshot;
    private final byte[]   prefix;

    private static final byte[]  META_PREFIX = new byte[] {'m'};

    private static final byte HASH_META_FLAG = 'H';
    private static final byte HASH_DATA_FLAG = 'h';
    private static final byte STR_META_FLAG = 'S';
    private static final byte STR_DATA_FLAG = 's';

    public CatalogTrasaction(Snapshot snapshot) {
        this.snapshot = snapshot;
        this.prefix = META_PREFIX;
    }

    private Snapshot getSnapshot() {
        return snapshot;
    }

    public byte[] getPrefix() {
        return prefix;
    }

    private static void encodeStringDataKey(CodecDataOutput cdo, byte[] prefix, byte[] key) {
        cdo.write(prefix);
        BytesUtils.writeBytes(cdo, key);
        LongUtils.writeULong(cdo, STR_DATA_FLAG);
    }

    private static void encodeHashDataKey(CodecDataOutput cdo, byte[] prefix, byte[] key, byte[] field) {
        encodeHashDataKeyPrefix(cdo, prefix, key);
        BytesUtils.writeBytes(cdo, field);
    }

    private static void encodeHashDataKeyPrefix(CodecDataOutput cdo, byte[] prefix, byte[] key) {
        cdo.write(prefix);
        cdo.write(key);
        BytesUtils.writeBytes(cdo, key);
        LongUtils.writeULong(cdo, HASH_DATA_FLAG);
    }

    public ByteString hashGet(ByteString key, ByteString field) {
        CodecDataOutput cdo = new CodecDataOutput();
        encodeHashDataKey(cdo, prefix, key.toByteArray(), field.toByteArray());
        return snapshot.get(cdo.toByteString());
    }

    public List<Pair<ByteString, ByteString>> hashGetFields(ByteString key) {
        CodecDataOutput cdo = new CodecDataOutput();
        encodeHashDataKeyPrefix(cdo, prefix, key.toByteArray());
        // Copy to a list just in case since iterator cannot rewind
        return Lists.newArrayList(Iterables.transform(IterableItertor.create(snapshot.scan(cdo.toByteString())),
                                                      kv -> Pair.create(kv.getKey(), kv.getValue())));
    }

    public ByteString getBytesValue(ByteString key) {
        CodecDataOutput cdo = new CodecDataOutput();
        encodeStringDataKey(cdo, key.toByteArray(), prefix);
        return snapshot.get(cdo.toByteString());
    }

    public long getLongValue(ByteString key) {
        return Long.parseLong(snapshot.get(key).toString());
    }
}
