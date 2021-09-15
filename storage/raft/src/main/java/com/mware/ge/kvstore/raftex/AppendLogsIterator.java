package com.mware.ge.kvstore.raftex;

import com.mware.ge.kvstore.raftex.RaftPart.LogTuple;
import com.mware.ge.kvstore.raftex.RaftPart.LogType;
import com.mware.ge.kvstore.utils.LogIterator;
import com.mware.ge.util.Preconditions;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public class AppendLogsIterator implements LogIterator {
    private long firstLogId_;
    private long logId_;
    private long termId_;
    private List<LogTuple> logs_;
    private OpProcessor opCB_;
    boolean leadByAtomicOp_ = false;
    boolean hasNonAtomicOpLogs_ = false;
    int idx_ = 0;
    boolean valid_ = true;
    LogType lastLogType_ = LogType.NORMAL;
    LogType currLogType_ = LogType.NORMAL;
    Optional<String> opResult_;

    public AppendLogsIterator(long firstLogId, long termId, List<LogTuple> logs, OpProcessor opCB) {
        this.firstLogId_ = firstLogId;
        this.logId_ = firstLogId;
        this.termId_ = termId;
        this.logs_ = logs;
        this.opCB_ = opCB;

        leadByAtomicOp_ = processAtomicOp();
        valid_ = idx_ < logs_.size();
        hasNonAtomicOpLogs_ = !leadByAtomicOp_ && valid_;
        if (valid_) {
            currLogType_ = lastLogType_ = logType();
        }
    }

    // Return true if the current log is a AtomicOp, otherwise return false
    boolean processAtomicOp() {
        while (idx_ < logs_.size()) {
            LogTuple tup = logs_.get(idx_);
            LogType logType = tup.logType;
            if (logType != LogType.ATOMIC_OP) {
                // Not a AtomicOp
                return false;
            }

            // Process AtomicOp log
            Preconditions.checkNotNull(opCB_);

            opResult_ = opCB_.apply(tup.atomicOp);
            if (opResult_.isPresent()) {
                // AtomicOp Succeeded
                return true;
            } else {
                // AtomicOp failed, move to the next log, but do not increment the logId_
                ++idx_;
            }
        }
        // Reached the end
        return false;
    }

    boolean leadByAtomicOp() {
        return leadByAtomicOp_;
    }

    boolean hasNonAtomicOpLogs() {
        return hasNonAtomicOpLogs_;
    }

    long firstLogId() {
        return firstLogId_;
    }

    @Override
    public LogIterator next() {
        ++idx_;
        ++logId_;
        if (idx_ < logs_.size()) {
            currLogType_ = logType();
            valid_ = currLogType_ != LogType.ATOMIC_OP;
            if (valid_) {
                hasNonAtomicOpLogs_ = true;
            }
            valid_ = valid_ && lastLogType_ != LogType.COMMAND;
            lastLogType_ = currLogType_;
        } else {
            valid_ = false;
        }
        return this;
    }

    // The iterator becomes invalid when exhausting the logs
    // **OR** running into a AtomicOp log
    @Override
    public boolean valid() {
        return valid_;
    }

    @Override
    public long logId() {
        Preconditions.checkState(valid());
        return logId_;
    }

    @Override
    public long logTerm() {
        Preconditions.checkState(valid());
        return termId_;
    }

    @Override
    public long logSource() {
        Preconditions.checkState(valid());
        return logs_.get(idx_).clusterId;
    }

    @Override
    public byte[] logMsg() {
        Preconditions.checkState(valid());
        if (currLogType_ == LogType.ATOMIC_OP) {
            Preconditions.checkState(opResult_.isPresent());
            return opResult_.get().getBytes(StandardCharsets.UTF_8);
        } else {
            return logs_.get(idx_).msg;
        }
    }

    // Return true when there is no more log left for processing
    boolean empty() {
        return idx_ >= logs_.size();
    }

    // Resume the iterator so that we can continue to process the remaining logs
    void resume() {
        Preconditions.checkState(!valid_);
        if (!empty()) {
            leadByAtomicOp_ = processAtomicOp();
            valid_ = idx_ < logs_.size();
            hasNonAtomicOpLogs_ = !leadByAtomicOp_ && valid_;
            if (valid_) {
                currLogType_ = lastLogType_ = logType();
            }
        }
    }

    LogType logType() {
        return logs_.get(idx_).logType;
    }

    @Override
    public void close() {
    }
}
