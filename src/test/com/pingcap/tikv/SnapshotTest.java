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
import com.pingcap.tikv.meta.TableInfo;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;


public class SnapshotTest {
    @Test
    public void testCreate() throws Exception {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.DEBUG);
        ctx.updateLoggers();

        TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
        TiSession session = TiSession.create(conf);
        PDClient client = PDClient.createRaw(session);
        RegionManager mgr = new RegionManager(client);
        Snapshot snapshot = new Snapshot(mgr, session);
        Catalog cat = new Catalog(snapshot);
        List<DBInfo> dbInfoList = cat.listDatabases();
        for (DBInfo dbInfo : dbInfoList) {
            List<TableInfo> tableInfoList = cat.listTables(dbInfo);
            if (tableInfoList.size() != 0) {
                TableInfo t = tableInfoList.get(0);
                System.out.println(t.getId());
            }
        }
        return;
    }
}