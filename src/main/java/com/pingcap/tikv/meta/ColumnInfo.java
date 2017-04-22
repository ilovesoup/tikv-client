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

package com.pingcap.tikv.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.pingcap.tikv.TiClientInternalException;
import com.pingcap.tikv.type.FieldType;
import com.pingcap.tikv.type.LongType;
import com.pingcap.tikv.type.StringType;

import javax.activation.UnsupportedDataTypeException;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnInfo {
    private final long        id;
    private final String      name;
    private final int         offset;
    private final FieldType   type;
    private final SchemaState schemaState;
    private final String      comment;

    @JsonCreator
    public ColumnInfo(@JsonProperty("id")long                   id,
                      @JsonProperty("name")CIStr                name,
                      @JsonProperty("offset")int                offset,
                      @JsonProperty("type")InternalTypeHolder   type,
                      @JsonProperty("state")int                 schemaState,
                      @JsonProperty("comment")String            comment) {
        this.id = id;
        this.name = name.getL();
        this.offset = offset;
        this.type = type.toFieldType();
        this.schemaState = SchemaState.fromValue(schemaState);
        this.comment = comment;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getOffset() {
        return offset;
    }

    public FieldType getType() {
        return type;
    }

    public SchemaState getSchemaState() {
        return schemaState;
    }

    public String getComment() {
        return comment;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class InternalTypeHolder {
        private final int         tp;
        private final int         flag;
        private final int         flen;
        private final int         decimal;
        private final String      charset;
        private final String      collate;

        private static final int TYPE_LONG        = 1;
        private static final int TYPE_STRING      = 0xfe;
        private static final int MASK_UNSIGNED    = 0x20;

        @JsonCreator
        public InternalTypeHolder(@JsonProperty("Tp")int                tp,
                                  @JsonProperty("Flag")int              flag,
                                  @JsonProperty("Flen")int              flen,
                                  @JsonProperty("Decimal")int           decimal,
                                  @JsonProperty("Charset")String        charset,
                                  @JsonProperty("Collate")String        collate) {
            this.tp = tp;
            this.flag = flag;
            this.flen = flen;
            this.decimal = decimal;
            this.charset = charset;
            this.collate = collate;
        }

        public int getTp() {
            return tp;
        }

        public int getFlag() {
            return flag;
        }

        public int getFlen() {
            return flen;
        }

        public int getDecimal() {
            return decimal;
        }

        public String getCharset() {
            return charset;
        }

        public String getCollate() {
            return collate;
        }

        public FieldType toFieldType() {
            switch (tp) {
                case TYPE_LONG:
                    if ((flag & MASK_UNSIGNED) != 0) {
                        return LongType.ULONG_TYPE;
                    } else {
                        return LongType.LONG_TYPE;
                    }
                case TYPE_STRING:
                    return StringType.STRING_TYPE;
                default:
                    // TODO: Handle other types
                    // throw new TiClientInternalException("Unsupported type code:" + tp);
                    return null;
            }
        }
    }
}
