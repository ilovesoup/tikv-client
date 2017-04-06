package com.pingcap.tikv.meta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.grpc.Metapb.Peer;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.RegionEpoch;

import java.util.List;

public class TiRegion {
    private final long id;
    private final ByteString               startKey;
    private final ByteString               endKey;
    private final TiRegionEpoch            regionEpoch;
    private final List<TiPeer>             peers;
    private final TiPeer                   leader;

    private TiRegion(long id, ByteString startKey, ByteString endKey,
                     TiRegionEpoch regionEpoch, List<TiPeer> peers,
                     Peer leader) {
        this.id = id;
        this.startKey = startKey;
        this.endKey = endKey;
        this.regionEpoch = regionEpoch;
        this.peers = peers;
        if (leader != null) {
            for (TiPeer peer : peers) {
                if (peer.getId() == leader.getId() && peer.getStoreId() == leader.getStoreId()) {
                    this.leader = peer;
                    return;
                }
            }
        }
        this.leader = null;
    }

    public static TiRegion parseFrom(Region r, Peer leader) {
        TiRegion region = new TiRegion(r.getId(),
                            r.getStartKey(),
                            r.getEndKey(),
                            TiRegionEpoch.parseFrom(r.getRegionEpoch()),
                            ImmutableList.copyOf(
                                    Iterables.transform(r.getPeersList(), (Peer peer) -> TiPeer.parseFrom(peer))),
                            leader
        );
        return region;
    }

    public long getId() {
        return id;
    }

    public ByteString getStartKey() {
        return startKey;
    }

    public ByteString getEndKey() {
        return endKey;
    }

    public TiRegionEpoch getRegionEpoch() {
        return regionEpoch;
    }

    public List<TiPeer> getPeers() { return peers; }

    public TiPeer getLeader() { return leader; }


    public static final class TiRegionEpoch {
        private final long confVer;
        private final long version;

        public static TiRegionEpoch parseFrom(RegionEpoch re) {
            return new TiRegionEpoch(re.getConfVer(), re.getVersion());
        }

        private TiRegionEpoch(long confVer, long version) {
            this.confVer = confVer;
            this.version = version;
        }

        public long getConfVer() {
            return confVer;
        }

        public long getVersion() {
            return version;
        }
    }

    public static final class TiPeer {
        private final long      id;
        private final long      storeId;

        public static TiPeer parseFrom(Peer p) {
            return new TiPeer(p.getId(), p.getStoreId());
        }

        private TiPeer(long id, long storeId) {
            this.id = id;
            this.storeId = storeId;
        }

        public long getId() {
            return id;
        }

        public long getStoreId() {
            return storeId;
        }
    }
}
