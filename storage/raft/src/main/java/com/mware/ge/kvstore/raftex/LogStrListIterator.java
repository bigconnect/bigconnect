package com.mware.ge.kvstore.raftex;

import com.mware.ge.kvstore.utils.LogIterator;

import java.util.List;

public class LogStrListIterator implements LogIterator {
    private long firstLogId;
    private long term;
    private int idx;
    private List<LogEntry> logEntries;

    public LogStrListIterator(long firstLogId, long term, List<LogEntry> logEntries) {
        this.firstLogId = firstLogId;
        this.term = term;
        this.logEntries = logEntries;
        this.idx = 0;
    }

    @Override
    public LogIterator next() {
        ++idx;
        return this;
    }

    @Override
    public boolean valid() {
        return idx < logEntries.size();
    }

    @Override
    public long logId() {
        assert valid();
        return firstLogId + idx;
    }

    @Override
    public long logTerm() {
        assert valid();
        return term;
    }

    @Override
    public long logSource() {
        assert valid();
        return logEntries.get(idx).getCluster();
    }

    @Override
    public byte[] logMsg() {
        assert valid();
        return logEntries.get(idx).getLog_str();
    }

    @Override
    public void close() {

    }
}
