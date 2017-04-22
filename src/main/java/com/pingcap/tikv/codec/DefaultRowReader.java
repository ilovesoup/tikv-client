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

package com.pingcap.tikv.codec;

import com.pingcap.tikv.meta.ObjectRowImpl;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.type.FieldType;

public class DefaultRowReader {
    private final CodecDataInput cdi;

    public DefaultRowReader create(CodecDataInput cdi) {
        return new DefaultRowReader(cdi);
    }

    private DefaultRowReader(CodecDataInput cdi) {
        this.cdi = cdi;
    }

    public Row readRow(FieldType[] fieldTypes) {
        Row row = ObjectRowImpl.create(fieldTypes.length);
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldTypes[i].decodeValueToRow(cdi, row, i);
        }
        return row;
    }
}
