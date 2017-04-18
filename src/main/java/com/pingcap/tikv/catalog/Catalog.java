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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiClientInternalException;
import com.pingcap.tikv.meta.DBInfo;
import com.pingcap.tikv.meta.TableInfo;
import com.pingcap.tikv.util.Pair;

import java.util.List;


public class Catalog {
    private static ByteString KEY_DB = ByteString.copyFromUtf8("DBs");
    private static ByteString KEY_TABLE = ByteString.copyFromUtf8("Table");

    private static String DB_PREFIX = "DB";

    private CatalogTrasaction trx;

    public Catalog(Snapshot snapshot) {
        trx = new CatalogTrasaction(snapshot);
    }

    public List<DBInfo> listDatabases() {
        List<Pair<ByteString, ByteString>> result = trx.hashGetFields(KEY_DB);
        ImmutableList.Builder<DBInfo> dbs = ImmutableList.builder();
        for (Pair<ByteString, ByteString> p : result) {
            dbs.add(parseFromJson(p.second, DBInfo.class));
        }
        return dbs.build();
    }

    public DBInfo getDatabase(long id) {
        return getDatabase(encodeId(id));
    }

    public List<TableInfo> listTables(DBInfo db) {
        ByteString dbKey = encodeId(db.getId());
        if (databaseExists(dbKey)) {
            throw new TiClientInternalException("Database not exists: " + db.getName());
        }

        List<Pair<ByteString, ByteString>> result = trx.hashGetFields(dbKey);
        ImmutableList.Builder<TableInfo> tables = ImmutableList.builder();
        for (Pair<ByteString, ByteString> p : result) {
            tables.add(parseFromJson(p.second, TableInfo.class));
        }
        return tables.build();
    }

    public DBInfo getDatabase(ByteString dbKey) {
        try {
            ByteString json = trx.hashGet(KEY_DB, dbKey);
            return parseFromJson(json, DBInfo.class);

        } catch (Exception e) {
            // TODO: Handle key not exists and let loose others
            return null;
        }
    }

    private static ByteString encodeId(long id) {
        return ByteString.copyFrom(String
                .format("%s:%d", DB_PREFIX, id)
                .getBytes());
    }

    private boolean databaseExists(ByteString dbKey) {
        return getDatabase(dbKey) == null;
    }

    private static <T> T parseFromJson(ByteString json, Class<T> cls) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(json.toString(), cls);
        } catch (JsonParseException | JsonMappingException e) {
            throw new TiClientInternalException("Invalid JSON value:\n" + json, e);
        } catch (Exception e1) {
            throw new TiClientInternalException("Error parsing Json", e1);
        }
    }
}
