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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.*;


public class DBInfoTest {
    @Test
    public void testSerialize() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = "{\n" +
                "\t\"id\": 1,\n" +
                "\t\"db_name\": \"testDb\",\n" +
                "\t\"charset\": \"utf8\",\n" +
                "\t\"collate\": \"x\",\n" +
                "\t\"-\": [{\"id\": 11, \"name\": \"testtable\", \"charset\": \"utf9\", \"collate\": \"x\"}, \n" +
                "\t\t  {\"id\": 12, \"name\": \"testtable1\", \"charset\": \"utf10\", \"collate\": \"x\"}],\n" +
                "\t\"state\": 1\n" +
                "}";
        DBInfo dbInfo = mapper.readValue(json, DBInfo.class);
    }
}