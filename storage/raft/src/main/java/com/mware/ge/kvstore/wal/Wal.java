package com.mware.ge.kvstore.wal;

import com.mware.ge.kvstore.utils.LogIterator;

/**
 * Base class for all WAL implementations
 */
public interface Wal {
    // Return the ID of the first log message in the WAL
    long firstLogId();

    // Return the ID of the last log message in the WAL
    long lastLogId();

    // Return the term to receive the last log
    long lastLogTerm();

    // Append one log message to the WAL
    boolean appendLog(long id, long term, long cluster, byte[] msg);

    // Append a list of log messages to the WAL
    boolean appendLogs(LogIterator iter);

    // Rollback to the given id, all logs after the id will be discarded
    boolean rollbackToLog(long id);

    // Create hard link for current wal on the new path.
    boolean linkCurrentWAL(String newPath);

    // Clean all wal files
    boolean reset();

    // clean time expired wal of wal_ttl
    void cleanWAL();

    // clean the wal before given log id
    void cleanWAL(long id);

    LogIterator iterator(long firstLogId, long lastLogId);
}
