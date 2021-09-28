package com.mware.ge.thrift;

import com.mware.ge.GeException;
import com.mware.ge.kvstore.raftex.RaftexService;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSocket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class ThriftClientManager {
    private Map<InetSocketAddress, RaftexService.Client> clientMap = new HashMap<>();

    public ThriftClientManager() {
    }

    public RaftexService.Client client(InetSocketAddress addr, int timeout) {
        if (clientMap.containsKey(addr)) {
            RaftexService.Client c = clientMap.get(addr);
            return c;
        }

        try {
            TSocket transport = new TSocket(addr.getHostString(), addr.getPort());
            transport.open();
            RaftexService.Client c = new RaftexService.Client.Factory().getClient(new TBinaryProtocol(transport));

            clientMap.put(addr, c);
            return c;
        } catch (Exception e) {
            e.printStackTrace();
            throw new GeException("Could not create raft client to: " + addr);
        }
    }
}
