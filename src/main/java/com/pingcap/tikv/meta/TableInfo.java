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

@JsonIgnoreProperties(ignoreUnknown = true)
public class TableInfo {
    private long        id;
    private String      name;
    private String      charset;
    private String      collate;

    @JsonCreator
    public TableInfo(@JsonProperty("id")long               id,
                     @JsonProperty("name")String           name,
                     @JsonProperty("charset")String        charset,
                     @JsonProperty("collate")String        collate) {
        this.id = id;
        this.name = name;
        this.charset = charset;
        this.collate = collate;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getCharset() {
        return charset;
    }

    public String getCollate() {
        return collate;
    }
}
