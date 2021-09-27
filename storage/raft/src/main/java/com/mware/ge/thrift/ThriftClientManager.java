package com.mware.ge.thrift;

import com.mware.ge.GeException;
import com.mware.ge.kvstore.raftex.RaftexService;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TNonblockingSocket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class ThriftClientManager {
    private Map<InetSocketAddress, RaftexService.AsyncClient> clientMap = new HashMap<>();
    private TAsyncClientManager asyncClientManager;
    private static ThriftClientManager INSTANCE;

    public static ThriftClientManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ThriftClientManager();
        }
        return INSTANCE;
    }

    private ThriftClientManager() {
        try {
            asyncClientManager = new TAsyncClientManager();
        } catch (IOException e) {
            throw new GeException("Could not create TAsyncClientManager", e);
        }
    }

    public RaftexService.AsyncClient client(InetSocketAddress addr, int timeout) {
        if (clientMap.containsKey(addr)) {
            RaftexService.AsyncClient c = clientMap.get(addr);
            return c;
        }

        try {
            TNonblockingSocket transport = new TNonblockingSocket(addr.getHostString(), addr.getPort(), timeout);
            transport.open();
            RaftexService.AsyncClient c = new RaftexService.AsyncClient.Factory(
                    asyncClientManager,
                    new TBinaryProtocol.Factory()
            ).getAsyncClient(transport);
            clientMap.put(addr, c);
            return c;
        } catch (Exception e) {
            throw new GeException("Could not create raft client to: " + addr);
        }
    }
}
