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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.pingcap.tikv.grpc.Metapb;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.grpc.Metapb.StoreLabel;
import com.pingcap.tikv.grpc.Metapb.StoreState;

import java.util.List;


public class TiStore {
    private long id;
    private String address;
    private StoreState state;
    private List<TiStoreLabel> labels;

    public static TiStore parseFrom(Store store) {
        return new TiStore(store.getId(),
                           store.getAddress(),
                           store.getState(),
                           ImmutableList.copyOf(
                                Iterables.transform(store.getLabelsList(),
                                                    (StoreLabel l) -> TiStoreLabel.parseFrom(l)))
        );
    }

    private TiStore(long id, String address, Metapb.StoreState state, List<TiStoreLabel> labels) {
        this.id = id;
        this.address = address;
        this.state = state;
        this.labels = labels;
    }

    public long getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public StoreState getState() {
        return state;
    }

    public List<TiStoreLabel> getLabels() {
        return labels;
    }

    public static final class TiStoreLabel {
        private final String key;
        private final String value;

        public static TiStoreLabel parseFrom(StoreLabel label) {
            return new TiStoreLabel(label.getKey(), label.getValue());
        }

        private TiStoreLabel(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }
    }
}
