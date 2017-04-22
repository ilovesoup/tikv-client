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

package com.pingcap.tikv.type;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.meta.Row;

public class LongType extends FieldType {

    private final boolean unsigned;

    public static final LongType LONG_TYPE = new LongType(false);
    public static final LongType ULONG_TYPE = new LongType(true);

    private LongType(boolean unsigned) {
        this.unsigned = unsigned;
    }

    @Override
    public void decodeValueNoNullToRow(CodecDataInput cdi, Row row, int pos) {
        if (unsigned) {
            // NULL should be checked outside
            row.setULong(pos, LongUtils.readULong(cdi));
        } else {
            row.setLong(pos, LongUtils.readLong(cdi));
        }
    }

    @Override
    public boolean isValidFlag(byte flag) {
        if (unsigned) {
            return flag == LongUtils.UINT_FLAG || flag == LongUtils.UVARINT_FLAG;
        } else {
            return flag == LongUtils.INT_FLAG || flag == LongUtils.VARINT_FLAG;
        }
    }

    @Override
    public String toString() {
        return unsigned ? "Unsigned" : "Signed" + "_LongType";
    }
}
