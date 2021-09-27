package com.mware.ge.kvstore.raftex;

import com.mware.ge.GeException;
import com.mware.ge.collection.Pair;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class RaftexServiceImpl implements RaftexService.Iface, RaftexService.AsyncIface {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(RaftexServiceImpl.class);

    public static final int STATUS_NOT_RUNNING = 0;
    public static final int STATUS_RUNNING = 2;

    TThreadPoolServer server_;
    int serverPort_;
    Thread serverThread_;
    AtomicInteger status_ = new AtomicInteger(STATUS_NOT_RUNNING);
    ReentrantLock partsLock_ = new ReentrantLock();
    Map<Pair<Integer, Integer>, RaftPart> parts_ = new HashMap();

    public static RaftexServiceImpl createService(int port) {
        RaftexServiceImpl service = new RaftexServiceImpl();
        try {
            service.initThriftServer(port);
            return service;
        } catch (TTransportException e) {
            throw new GeException("Could not setup Raft server, aborting.", e);
        }
    }

    private RaftexServiceImpl() {
    }

    private void initThriftServer(int port) throws TTransportException {
        LOGGER.info("Init thrift server for raft service, port: " + port);
        serverPort_ = port;
        TServerTransport transport = new TServerSocket(port);
        server_ = new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                .processor(new RaftexService.Processor<>(this)));
    }

    public boolean start() {
        serverThread_ = new Thread(() -> serve());
        waitUntilReady();
        if (status_.get() != STATUS_RUNNING) {
            waitUntilStop();
            return false;
        }
        return true;
    }

    public void stop() {
        if (status_.get() != STATUS_RUNNING) {
            return;
        }

        LOGGER.info("Stopping the Raftex service on port: " + serverPort_);
        try {
            partsLock_.lock();
            for (RaftPart p : parts_.values())
                p.stop();

            parts_.clear();
            LOGGER.info("All partitions have stopped");
        } finally {
            partsLock_.unlock();
        }

        server_.stop();
    }

    private void waitUntilStop() {
        if (serverThread_ != null) {
            try {
                serverThread_.join();
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void serve() {
        LOGGER.info("Starting the Raftex Service");

        status_.set(STATUS_RUNNING);
        server_.serve();

        status_.set(STATUS_NOT_RUNNING);
        LOGGER.info("The Raftex Service stopped");
    }

    private void waitUntilReady() {
        while (status_.get() == STATUS_NOT_RUNNING) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException ignored) {
            }
        }
    }

    public void addPartition(RaftPart part) {
        try {
            partsLock_.lock();
            parts_.put(Pair.of(part.spaceId(), part.partitionId()), part);
        } finally {
            partsLock_.unlock();
        }
    }

    public void removePartition(RaftPart part) {
        try {
            partsLock_.lock();
            RaftPart removed = parts_.remove(Pair.of(part.spaceId(), part.partitionId()));
            removed.stop();
        } finally {
            partsLock_.unlock();
        }
    }

    public RaftPart findPart(int spaceId, int partId) {
        try {
            partsLock_.lock();
            RaftPart part = parts_.getOrDefault(Pair.of(spaceId, partId), null);
            if (part == null) {
                LOGGER.warn("Cannot find the part "+partId+" in the graph space "+spaceId);
            }
            return part;
        } finally {
            partsLock_.unlock();
        }
    }


    @Override
    public AskForVoteResponse askForVote(AskForVoteRequest req) throws TException {
        RaftPart part = findPart(req.getSpace(), req.getPart());
        AskForVoteResponse resp = new AskForVoteResponse();
        if (part == null) {
            resp.setError_code(ErrorCode.E_UNKNOWN_PART);
            return resp;
        }

        part.processAskForVoteRequest(req, resp);
        return resp;
    }

    @Override
    public AppendLogResponse appendLog(AppendLogRequest req) throws TException {
        RaftPart part = findPart(req.getSpace(), req.getPart());
        AppendLogResponse resp = new AppendLogResponse();
        if (part == null) {
            resp.setError_code(ErrorCode.E_UNKNOWN_PART);
            return resp;
        }
        part.processAppendLogRequest(req, resp);
        return resp;
    }

    @Override
    public SendSnapshotResponse sendSnapshot(SendSnapshotRequest req) throws TException {
        RaftPart part = findPart(req.getSpace(), req.getPart());
        SendSnapshotResponse resp = new SendSnapshotResponse();
        if (part == null) {
            resp.setError_code(ErrorCode.E_UNKNOWN_PART);
            return resp;
        }
        part.processSendSnapshotRequest(req, resp);
        return resp;
    }

    @Override
    public HeartbeatResponse heartbeat(HeartbeatRequest req) throws TException {
        RaftPart part = findPart(req.getSpace(), req.getPart());
        HeartbeatResponse resp = new HeartbeatResponse();
        if (part == null) {
            resp.setError_code(ErrorCode.E_UNKNOWN_PART);
            return resp;
        }
        part.processHeartbeatRequest(req, resp);
        return resp;
    }

    @Override
    public void askForVote(AskForVoteRequest req, AsyncMethodCallback<AskForVoteResponse> resultHandler) throws TException {
        resultHandler.onComplete(askForVote(req));
    }

    @Override
    public void appendLog(AppendLogRequest req, AsyncMethodCallback<AppendLogResponse> resultHandler) throws TException {
        resultHandler.onComplete(appendLog(req));
    }

    @Override
    public void sendSnapshot(SendSnapshotRequest req, AsyncMethodCallback<SendSnapshotResponse> resultHandler) throws TException {
        resultHandler.onComplete(sendSnapshot(req));
    }

    @Override
    public void heartbeat(HeartbeatRequest req, AsyncMethodCallback<HeartbeatResponse> resultHandler) throws TException {
        resultHandler.onComplete(heartbeat(req));
    }
}
