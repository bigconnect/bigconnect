package com.mware.ge.kvstore.utils;

public interface LogIterator extends AutoCloseable {
    LogIterator next();
    boolean valid();
    long logId();
    long logTerm();
    long logSource();
    byte[] logMsg();
}
