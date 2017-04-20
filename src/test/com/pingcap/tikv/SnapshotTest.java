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

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.catalog.CatalogTrasaction;
import com.pingcap.tikv.meta.DBInfo;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;


public class SnapshotTest {
    @Test
    public void testCreate() throws Exception {
        TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
        TiSession session = TiSession.create(conf);
        PDClient client = PDClient.createRaw(session);
        RegionManager mgr = new RegionManager(client);
        Snapshot snapshot = new Snapshot(mgr, session);
        Catalog cat = new Catalog(snapshot);
        List<DBInfo> dbInfoList = cat.listDatabases();
        return;
    }
}