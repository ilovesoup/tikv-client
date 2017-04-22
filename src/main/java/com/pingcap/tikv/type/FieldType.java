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


import com.pingcap.tikv.TiClientInternalException;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.Row;

public abstract class FieldType {
    private static final byte NULL_FLAG = 0;

    protected abstract void decodeValueNoNullToRow(CodecDataInput cdi, Row row, int pos);

    public void decodeValueToRow(CodecDataInput cdi, Row row, int pos) {
        byte flag = cdi.readByte();
        if (isNullFlag(flag)) {
            row.setNull(pos);
        }
        if (!isValidFlag(flag)) {
            throw new TiClientInternalException("Invalid " + toString() + " flag: " + flag);
        }
        decodeValueNoNullToRow(cdi, row, pos);
    }

    protected abstract boolean isValidFlag(byte flag);

    protected boolean isNullFlag(byte flag) {
        return flag == NULL_FLAG;
    }

    @Override
    public abstract String toString();
}
