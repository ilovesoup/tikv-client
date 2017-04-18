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

import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.meta.DBInfo;


public class Catalog {
    private static ByteString KEY_DB = ByteString.copyFromUtf8("DBs");
    private static ByteString KEY_TABLE = ByteString.copyFromUtf8("Table");

    private CatalogTrasaction trx;

    public Catalog(Snapshot snapshot) {
        trx = new CatalogTrasaction(snapshot);
    }

    public DBInfo listDatabases() {
        trx.hashGetFields(KEY_DB);
        return null;
    }
}
