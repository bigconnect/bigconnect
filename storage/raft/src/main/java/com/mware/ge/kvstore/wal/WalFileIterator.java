package com.mware.ge.kvstore.wal;

import com.mware.ge.GeException;
import com.mware.ge.collection.Pair;
import com.mware.ge.io.IOUtils;
import com.mware.ge.kvstore.utils.LogIterator;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WalFileIterator implements LogIterator, AutoCloseable {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(WalFileIterator.class);

    FileBasedWal wal;
    long lastId;
    long currId;
    long currTerm;
    long nextFirstId;

    List<Pair<Long, Long>> idRanges = new ArrayList<>();
    long currPos = 0;
    int currMsgLen = 0;
    byte[] currLog;
    List<RandomAccessFile> fds = new ArrayList<>();

    // The range is [startId, lastId]
    // if the lastId < 0, the wal_->lastId_ will be used
    public WalFileIterator(FileBasedWal wal, long startId, long lastId) {
        this.wal = wal;
        this.currId = startId;

        if (lastId >= 0 && lastId <= wal.lastLogId()) {
            this.lastId = lastId;
        } else {
            this.lastId = wal.lastLogId();
        }

        if (this.currId > this.lastId) {
            LOGGER.error("%s The log %d is out of range, the lastLogId is %d", this.wal.idStr, this.currId, this.lastId);
            return;
        }

        if (startId < this.wal.firstLogId()) {
            LOGGER.error("%s The given log id  %d is out of range, the wal firstLogId is %d", startId, this.wal.firstLogId());
            this.currId = this.lastId + 1;
            return;
        }

        // We need to read from the WAL files
        this.wal.accessAllWalInfo(info -> {
            try {
                fds.add(0, new RandomAccessFile(info.path(), "r"));
                idRanges.add(0, Pair.of(info.firstId(), info.lastId()));

                if (info.firstId() <= this.currId) {
                    // Go no further
                    return false;
                } else {
                    return true;
                }
            } catch (FileNotFoundException e) {
                LOGGER.error("Failed to open wal file %s", info.path(), e);
                this.currId = this.lastId + 1;
                return false;
            }
        });

        if (idRanges.isEmpty() || idRanges.get(0).first() > this.currId) {
            LOGGER.error("LogID %s is out of the wal files range", this.currId);
            this.currId = this.lastId + 1;
            return;
        }

        this.nextFirstId = getFirstIdInNextFile();
        if (this.currId > idRanges.get(0).other()) {
            LOGGER.error(wal.idStr + " currId " + this.currId
                    + ", idRanges.front firstLogId " + idRanges.get(0).first()
                    + ", idRanges.front lastLogId " + idRanges.get(0).other()
                    + ", idRanges size " + idRanges.size()
                    + ", lastId_ " + this.lastId
                    + ", nextFirstId_ " + this.nextFirstId
            );
        }

        if (!idRanges.isEmpty()) {
            // Find the correct position in the first WAL file
            this.currPos = 0;
            while (true) {
                RandomAccessFile raf = fds.get(0);
                try {
                    raf.seek(this.currPos);
                    // Read the logID
                    long logId = raf.readLong();

                    // Read the termID
                    raf.seek(this.currPos + Long.BYTES);
                    this.currTerm = raf.readLong();

                    // Read the log length
                    raf.seek(this.currPos + Long.BYTES + Long.BYTES);
                    this.currMsgLen = raf.readInt();

                    if (logId == this.currId) {
                        break;
                    }

                    this.currPos += Long.BYTES // logId
                            + Long.BYTES // termId
                            + Integer.BYTES * 2 // len
                            + this.currMsgLen
                            + Long.BYTES; // clusterId
                } catch (IOException e) {
                    throw new GeException("Problem reading wal file", e);
                }
            }
        }
    }

    @Override
    public LogIterator next() {
        ++currId;
        if (currId >= nextFirstId) {
            // Need to roll over to next file
            // Close the current file
            IOUtils.closeAllSilently(fds.get(0));
            fds.remove(0);
            idRanges.remove(0);

            if (idRanges.isEmpty()) {
                // Reached the end of wal files, only happens
                // when there is no buffer to read
                currId = lastId + 1;
                return this;
            }

            nextFirstId = getFirstIdInNextFile();
            Preconditions.checkState(currId == idRanges.get(0).first());
            currPos = 0;
        } else {
            // Move to the next log
            this.currPos += Long.BYTES // logId
                    + Long.BYTES // termId
                    + Integer.BYTES * 2 // len
                    + this.currMsgLen
                    + Long.BYTES; // clusterId
        }

        if (idRanges.get(0).other() <= 0) {
            // empty file
            currId = lastId + 1;
            return this;
        } else {
            RandomAccessFile raf = fds.get(0);
            try {
                raf.seek(this.currPos);
                // Read the logID
                long logId = raf.readLong();
                Preconditions.checkState(currId == logId);

                // Read the termID
                raf.seek(this.currPos + Long.BYTES);
                this.currTerm = raf.readLong();

                // Read the log length
                raf.seek(this.currPos + Long.BYTES + Long.BYTES);
                this.currMsgLen = raf.readInt();
            } catch (IOException e) {
                throw new GeException("Problem reading wal file", e);
            }
        }

        return this;
    }

    @Override
    public boolean valid() {
        return currId <= lastId;
    }

    @Override
    public long logId() {
        return currId;
    }

    @Override
    public long logTerm() {
        return currTerm;
    }

    @Override
    public long logSource() {
        // Retrieve from the file
        assert !fds.isEmpty();
        RandomAccessFile raf = fds.get(0);
        try {
            raf.seek(this.currPos + Long.BYTES + Long.BYTES + Integer.BYTES);
            long cluster = raf.readLong();
            return cluster;
        } catch (IOException e) {
            throw new GeException("Problem reading wal file", e);
        }
    }

    @Override
    public byte[] logMsg() {
        // Retrieve from the file
        assert !fds.isEmpty();

        currLog = new byte[currMsgLen];

        RandomAccessFile raf = fds.get(0);
        try {
            raf.seek(this.currPos + Long.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES);
            raf.read(currLog);
            return currLog;
        } catch (IOException e) {
            throw new GeException("Problem reading wal file", e);
        }
    }

    private long getFirstIdInNextFile() {
        Iterator<Pair<Long, Long>> iter = idRanges.iterator();
        Pair<Long, Long> e = iter.next();
        if (!iter.hasNext()) {
            return e.other() + 1;
        } else {
            return iter.next().first();
        }
    }

    @Override
    public void close() {
        IOUtils.closeAllSilently(this.fds);
    }
}
