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
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.meta.TiTableInfo;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;


public class SnapshotTest {
    @Test
    public void testCreate() throws Exception {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.DEBUG);
        ctx.updateLoggers();

        TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
        TiCluster cluster = TiCluster.getCluster(conf);
        Snapshot snapshot = cluster.createSnapshot();
        Catalog cat = cluster.getCatalog();

        List<TiDBInfo> dbInfoList = cat.listDatabases();
        TiTableInfo table = null;
        for (TiDBInfo dbInfo : dbInfoList) {
            List<TiTableInfo> tableInfoList = cat.listTables(dbInfo);
            for (TiTableInfo t : tableInfoList) {
                if (t.getName().equals("t2")) {
                    table = t;
                }
            }
        }

        Iterator<Row> it = snapshot.newSelect()
                .addRange(TiRange.create(0L, Long.MAX_VALUE))
                .setTable(table)
                .doSelect();

        List<Row> rows = ImmutableList.copyOf(it);

        return;
    }
}