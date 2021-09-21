package com.mware.ge.kvstore.raftex;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public class SnapshotManager {
    public CompletableFuture<Status> sendSnapshot(RaftPart part, InetSocketAddress addr) {
        throw new UnsupportedOperationException("TBI");
    }
}
