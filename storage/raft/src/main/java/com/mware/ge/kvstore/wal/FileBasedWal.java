package com.mware.ge.kvstore.wal;

import com.mware.ge.GeException;
import com.mware.ge.collection.Pair;
import com.mware.ge.io.IOUtils;
import com.mware.ge.kvstore.DiskManager;
import com.mware.ge.kvstore.utils.LogIterator;
import com.mware.ge.time.Clocks;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class FileBasedWal implements Wal, AutoCloseable {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(FileBasedWal.class);

    // Default wal ttl
    public static int WAL_TTL = 14400;

    // firstLogId -> WalInfo
    // The last entry is the current opened WAL file
    LinkedList<Pair<Long, WalFileInfo>> walFiles = new LinkedList<>();
    Lock walFilesMutex = new ReentrantLock();

    String dir;
    String idStr;
    int spaceId = 0;
    int partId = 0;
    AtomicBoolean stopped = new AtomicBoolean(false);
    FileBasedWalPolicy policy = new FileBasedWalPolicy();
    long firstLogId = 0;
    long lastLogId = 0;
    long lastLogTerm = 0;


    // The WalFileInfo corresponding to the currFd
    WalFileInfo ccurrInfo;
    AtomicLogBuffer logBuffer;
    PreProcessor preProcessor;
    DiskManager diskMan;
    Lock rollbackLock = new ReentrantLock();

    OutputStream outputStream;

    public static FileBasedWal getWal(String dir, FileBasedWalInfo info, FileBasedWalPolicy policy, PreProcessor preProcessor, DiskManager diskMan) {
        return new FileBasedWal(dir, info, policy, preProcessor, diskMan);
    }

    // Callers **SHOULD NEVER** use this constructor directly
    // Callers should use static method getWal() instead
    private FileBasedWal(String dir, FileBasedWalInfo wInfo, FileBasedWalPolicy policy, PreProcessor preProcessor, DiskManager diskMan) {
        this.dir = dir;
        this.idStr = wInfo.idStr;
        this.spaceId = wInfo.spaceId;
        this.partId = wInfo.partId;
        this.policy = policy;
        this.preProcessor = preProcessor;
        this.diskMan = diskMan;

        // Make sure WAL directory exist
        File dirFolder = new File(dir);
        if (!dirFolder.exists()) {
            if (!dirFolder.mkdirs()) {
                throw new GeException("Could not create WAL directory: " + dir);
            }
        }

        logBuffer = AtomicLogBuffer.instance(policy.bufferSize);
        scanAllWalFiles();

        if (!walFiles.isEmpty()) {
            this.firstLogId = walFiles.get(0).other().firstId();
            WalFileInfo info = walFiles.get(walFiles.size() - 1).other();
            this.lastLogId = info.lastId();
            this.lastLogTerm = info.lastTerm();
            try {
                outputStream = new FileOutputStream(info.path(), true);
                ccurrInfo = info;
            } catch (FileNotFoundException e) {
                throw new GeException("Could not open WAL file " + info.path() + ": " + e.getMessage());
            }
        }
    }

    private void scanAllWalFiles() {
        Collection<File> files = FileUtils.listFiles(new File(dir), new String[]{"wal"}, false);
        for (File fn : files) {
            // Split the file name
            // The file name convention is "<first id in the file>.wal"
            String[] parts = StringUtils.split(fn.getName(), '.');
            if (parts.length != 2) {
                LOGGER.error("Skipping unknown file: %s", fn.getName());
                continue;
            }

            long startIdFromName;
            try {
                startIdFromName = Long.parseLong(parts[0]);
            } catch (NumberFormatException ex) {
                LOGGER.error("Skipping bad file name: %s", fn.getName());
                continue;
            }

            WalFileInfo info = new WalFileInfo(
                    Paths.get(dir, fn.getName()).toAbsolutePath().toString(),
                    startIdFromName
            );
            walFiles.add(Pair.of(startIdFromName, info));

            try {
                BasicFileAttributes attr = Files.readAttributes(fn.toPath(), BasicFileAttributes.class);
                info.setSize(attr.size());
                info.setMtime(attr.lastModifiedTime().toMillis());
            } catch (IOException e) {
                LOGGER.error("Could not get file attributes. Skipping file: %s", fn.getName());
                continue;
            }

            if (info.size() == 0) {
                // Found an empty WAL file
                LOGGER.warn("Found an empty WAL file: %s", fn.getName());
                info.setLastId(0);
                info.setLastTerm(0);
                continue;
            }

            // Open the file
            RandomAccessFile raf;
            long rafLength;
            try {
                raf = new RandomAccessFile(fn, "r");
                rafLength = raf.length();
            } catch (Exception e) {
                LOGGER.error("Failed to open WAL file %s: %s", fn.getName(), e.getMessage());
                continue;
            }

            // Read the first log id
            long firstLogId;
            try {
                firstLogId = raf.readLong();
            } catch (IOException e) {
                IOUtils.closeAllSilently(raf);
                LOGGER.error("Failed to read the first log id from %s", fn.getName());
                continue;
            }

            if (firstLogId != startIdFromName) {
                IOUtils.closeAllSilently(raf);
                LOGGER.error("The first log id %s does not match the file name %s", firstLogId, fn.getName());
                continue;
            }

            int succMsgLen;
            try {
                raf.seek(rafLength - Integer.BYTES);
                // Read the last log length
                succMsgLen = raf.readInt();
            } catch (IOException e) {
                IOUtils.closeAllSilently(raf);
                LOGGER.error("Failed to read the last log length from %s", fn.getName());
                continue;
            }

            // Verify the last log length
            int precMsgLen;
            try {
                raf.seek(rafLength - (Integer.BYTES * 2 + succMsgLen + Long.BYTES));
                precMsgLen = raf.readInt();
            } catch (IOException e) {
                IOUtils.closeAllSilently(raf);
                LOGGER.error("Failed to read the last log length from %s", fn.getName());
                continue;
            }

            if (precMsgLen != succMsgLen) {
                IOUtils.closeAllSilently(raf);
                LOGGER.error("It seems the wal file %s is corrupted. Ignore it", fn.getName());
                continue;
            }

            // Read the last log term
            long term;
            try {
                raf.seek(rafLength - (Integer.BYTES * 2 + succMsgLen + Long.BYTES + Long.BYTES));
                term = raf.readLong();
            } catch (IOException e) {
                IOUtils.closeAllSilently(raf);
                LOGGER.error("Failed to read the last log term from %s", fn.getName());
                continue;
            }

            info.setLastTerm(term);

            // Read the last log id
            long lastLogId;
            try {
                raf.seek(rafLength - (Integer.BYTES * 2 + succMsgLen + Long.BYTES + Long.BYTES + Long.BYTES));
                lastLogId = raf.readLong();
            } catch (IOException e) {
                IOUtils.closeAllSilently(raf);
                LOGGER.error("Failed to read the last log id from %s", fn.getName());
                continue;
            }
            info.setLastId(lastLogId);

            // We have everything we need
            IOUtils.closeAllSilently(raf);
        }

        if (!walFiles.isEmpty()) {
            walFiles.sort(Comparator.comparing(Pair::first));
        }

        if (!walFiles.isEmpty()) {
            // Try to scan last wal, if it is invalid or empty, scan the privous one
            Pair<Long, WalFileInfo> prev = walFiles.getLast();
            scanLastWal(prev.other(), prev.other().firstId());
            if (prev.other().lastId() <= 0) {
                FileUtils.deleteQuietly(new File(prev.other().path()));
                walFiles.removeLast();
            }
        }

        // Make sure there is no gap in the logs
        if (!walFiles.isEmpty()) {
            long logIdAfterLastGap = -1;
            ListIterator<Pair<Long, WalFileInfo>> iter = walFiles.listIterator();
            Pair<Long, WalFileInfo> firstRec = iter.next();
            long prevLastId = firstRec.other().lastId();
            while (iter.hasNext()) {
                Pair<Long, WalFileInfo> nextRec = iter.next();
                if (nextRec.other().firstId() > prevLastId + 1) {
                    // Found a gap
                    LOGGER.error("Found a log id gap before %d , the previous log id is %d", nextRec.other().firstId(), prevLastId);
                    logIdAfterLastGap = nextRec.other().firstId();
                }
                prevLastId = nextRec.other().lastId();
            }
            if (logIdAfterLastGap > 0) {
                // Found gap, remove all logs before the last gap
                iter = walFiles.listIterator();
                while (iter.hasNext()) {
                    Pair<Long, WalFileInfo> next = iter.next();
                    if (next.other().firstId() < logIdAfterLastGap)
                        iter.remove();
                }
            }
        }
    }

    private void scanLastWal(WalFileInfo info, long firstId) {
        try (RandomAccessFile raf = new RandomAccessFile(info.path(), "r")) {
            long curLogId = firstId;
            long pos = 0;
            long id = 0;
            long term = 0;
            int head = 0;
            int foot = 0;

            while (true) {
                // Read the log Id
                raf.seek(pos);
                try {
                    id = raf.readLong();
                } catch (EOFException eofex) {
                    break;
                }

                if (id != curLogId) {
                    LOGGER.error("LogId is not consistent %d %d", id, curLogId);
                    break;
                }

                // Read the term Id
                raf.seek(pos + Long.BYTES);
                term = raf.readLong();


                // Read the message length
                raf.seek(pos + Long.BYTES + Long.BYTES);
                head = raf.readInt();

                raf.seek(pos + Long.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES + head);
                foot = raf.readInt();

                if (head != foot) {
                    LOGGER.error("Message size doen't match: %d != %d", head, foot);
                    break;
                }

                info.setLastTerm(term);
                info.setLastId(id);

                // Move to the next log
                pos += Long.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES + head + Integer.BYTES;

                ++curLogId;
            }

            if (0 < pos && pos < new File(info.path()).length()) {
                LOGGER.warn("Invalid wal %s, , truncate from offset %d", info.path(), pos);
                try (FileChannel outChan = new FileOutputStream(info.path(), true).getChannel()) {
                    outChan.truncate(pos);
                }
                info.setSize(pos);
            }
        } catch (Exception e) {
            throw new GeException("Failed to open file " + info.path(), e);
        }
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public long firstLogId() {
        return firstLogId;
    }

    @Override
    public long lastLogId() {
        return lastLogId;
    }

    @Override
    public long lastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public LogIterator iterator(long firstLogId, long lastLogId) {
        AtomicLogBuffer.Iterator iter = logBuffer.iterator(firstLogId, lastLogId);
        if (iter.valid())
            return iter;

        return new WalFileIterator(this, firstLogId, lastLogId);
    }

    @Override
    public boolean appendLog(long id, long term, long cluster, byte[] msg) {
//        if (diskMan_ && !diskMan_->hasEnoughSpace(spaceId_, partId_)) {
//            LOG_EVERY_N(WARNING, 100) << idStr_ << "Failed to appendLogs because of no more space";
//            return false;
//        }
        if (!appendLogInternal(id, term, cluster, msg)) {
            LOGGER.error("Failed to append log for logId %d", id);
            return false;
        }
        return true;
    }

    @Override
    public boolean appendLogs(LogIterator iter) {
        return false;
    }

    private boolean appendLogInternal(long id, long term, long cluster, byte[] msg) {
        if (stopped.get()) {
            LOGGER.error("%s WAL has stopped. Do not accept logs any more", idStr);
            return false;
        }

        if (lastLogId != 0 && firstLogId != 0 && id != lastLogId + 1) {
            LOGGER.error("%s There is a gap in the log id. The last log id is %s, and the id being appended is %d", idStr, lastLogId, id);
            return false;
        }

        if (!preProcessor.apply(id, term, cluster, msg)) {
            LOGGER.error("%s Pre process failed for log %d", idStr, id);
            return false;
        }

        // Write to the WAL file first
        ByteBuffer strBuf = ByteBuffer.allocate(
                Long.BYTES  // sizeof(LogID)
                        + Long.BYTES  // sizeof(TermID)
                        + Long.BYTES // sizeof(ClusterID)
                        + msg.length
                        + 2 * Integer.BYTES
        );
        strBuf.putLong(id);
        strBuf.putLong(term);
        strBuf.putInt(msg.length);
        strBuf.putLong(cluster);
        strBuf.put(msg);
        strBuf.putInt(msg.length);
        byte[] buf = strBuf.array();

        // Prepare the WAL file if it's not opened
        if (outputStream == null) {
            prepareNewFile(id);
        } else if (ccurrInfo.size() + buf.length > policy.fileSize) {
            // Need to roll over
            closeCurrFile();

            try {
                walFilesMutex.lock();
                prepareNewFile(id);
            } finally {
                walFilesMutex.unlock();
            }
        }

        try {
            outputStream.write(buf);
            if (policy.sync) {
                outputStream.flush();
            }

            ccurrInfo.setSize(ccurrInfo.size() + buf.length);
            ccurrInfo.setLastId(id);
            ccurrInfo.setLastTerm(term);

            lastLogId = id;
            lastLogTerm = term;
            if (firstLogId == 0)
                firstLogId = id;

            logBuffer.push(id, term, cluster, msg);
            return true;
        } catch (IOException e) {
            throw new GeException("Could not write WAL: " + ccurrInfo.path(), e);
        }
    }

    private void closeCurrFile() {
        if (outputStream == null) {
            Preconditions.checkState(ccurrInfo == null);
            return;
        }

        try {
            outputStream.close();
            outputStream = null;
            Instant instant = Instant.now();
            ccurrInfo.setMtime(instant.toEpochMilli());
            BasicFileAttributeView fileAttrs =
                    Files.getFileAttributeView(Paths.get(ccurrInfo.path()), BasicFileAttributeView.class);
            fileAttrs.setTimes(FileTime.from(instant), null, null);
            ccurrInfo = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void prepareNewFile(long startLogId) {
        if (outputStream != null) {
            LOGGER.warn("The current file needs to be closed first");
        }

        WalFileInfo info = new WalFileInfo(
                Paths.get(dir, String.format("%d.wal", startLogId)).toString(),
                startLogId
        );

        walFiles.add(Pair.of(startLogId, info));

        try {
            outputStream = new FileOutputStream(info.path());
            ccurrInfo = info;
        } catch (FileNotFoundException e) {
            throw new GeException("Failed to open file " + info.path(), e);
        }
    }

    @Override
    public boolean rollbackToLog(long id) {
        if (id < firstLogId - 1 || id > lastLogId) {
            LOGGER.error(idStr + " Rollback target id " + id + "is not in the range of [ "
                    + firstLogId + "," + lastLogId + " of the WAL");
            return false;
        }

        try {
            rollbackLock.lock();

            //-----------------------
            // 1. Roll back WAL files
            //-----------------------

            // First close the current file
            closeCurrFile();

            try {
                walFilesMutex.lock();
                if (!walFiles.isEmpty()) {
                    Iterator<Pair<Long, WalFileInfo>> iter = walFiles.iterator();
                    while (iter.hasNext()) {
                        Pair<Long, WalFileInfo> e = iter.next();
                        if (e.first() > id) {
                            LOGGER.debug("Removing file " + e.other().path());
                            FileUtils.deleteQuietly(new File(e.other().path()));
                            iter.remove();
                        }
                    }
                }

                if (walFiles.isEmpty()) {
                    // All WAL files are gone
                    Preconditions.checkState(id == firstLogId - 1 || id == 0);
                    firstLogId = 0;
                    lastLogId = 0;
                    lastLogTerm = 0;
                } else {
                    rollbackInFile(walFiles.getLast().other(), id);
                }
            } finally {
                walFilesMutex.unlock();
            }

            //------------------------------
            // 2. Roll back in-memory buffers
            //------------------------------
            logBuffer.reset();

            return true;
        } finally {
            rollbackLock.unlock();
        }
    }

    private void rollbackInFile(WalFileInfo info, long logId) {
        try {
            RandomAccessFile raf = new RandomAccessFile(info.path(), "r");

            int pos = 0;
            long id = 0;
            long term = 0;
            while (true) {

                // Read the log Id
                try {
                    raf.seek(pos);
                    id = raf.readLong();
                } catch (IOException e) {
                    LOGGER.error("Failed to read the log id", e);
                    break;
                }

                // Read the term Id
                try {
                    raf.seek(pos + Long.BYTES);
                    term = raf.readLong();
                } catch (IOException e) {
                    LOGGER.error("Failed to read the term id", e);
                    break;
                }

                // Read the message length
                int len;
                try {
                    raf.seek(pos + Long.BYTES + Long.BYTES);
                    len = raf.readInt();
                } catch (IOException e) {
                    LOGGER.error("Failed to read message length", e);
                    break;
                }

                pos += Long.BYTES + Long.BYTES + Long.BYTES + 2 * Integer.BYTES + len;

                if (id == logId) {
                    break;
                }
            }

            if (id != logId) {
                LOGGER.error(idStr + " Didn't found log " + logId + " in " + info.path());
            }

            lastLogId = logId;
            lastLogTerm = term;

            LOGGER.info(idStr + " Rollback to log " + logId);
            Preconditions.checkState(pos > 0, "This wal should have been deleted");
            try {
                if (pos < raf.length()) {
                    LOGGER.info(idStr + " Need to truncate from offset " + pos);
                    try (FileChannel outChan = new FileOutputStream(info.path(), true).getChannel()) {
                        outChan.truncate(pos);
                    }
                    info.setSize(pos);
                }

                info.setLastId(id);
                info.setLastTerm(term);

                IOUtils.closeAllSilently(raf);
            } catch (IOException e) {
                throw new GeException("I/O error on file " + info.path(), e);
            }
        } catch (FileNotFoundException e) {
            throw new GeException("Failed to open file " + info.path(), e);
        }
    }

    @Override
    public boolean linkCurrentWAL(String newPath) {
        return false;
    }

    @Override
    public boolean reset() {
        closeCurrFile();
        logBuffer.reset();

        try {
            walFilesMutex.lock();
            walFiles.clear();
        } finally {
            walFilesMutex.unlock();
        }

        Collection<File> files = FileUtils.listFiles(new File(dir), new String[]{"wal"}, false);
        for (File fn : files) {
            LOGGER.info("Removing WAL file: %s", fn.getName());
            FileUtils.deleteQuietly(fn);
        }
        lastLogId = 0;
        firstLogId = 0;

        return true;
    }

    @Override
    public void cleanWAL() {
        try {
            walFilesMutex.lock();
            if (walFiles.isEmpty())
                return;

            long now = Clocks.systemClock().millis();
            // In theory we only need to keep the latest wal file because it is beging written now.
            // However, sometimes will trigger raft snapshot even only a small amount of logs is missing,
            // especially when we reboot all storage, so se keep one more wal.
            int index = 0;
            Iterator<Pair<Long, WalFileInfo>> it = walFiles.iterator();
            int size = walFiles.size();
            if (size < 2) {
                return;
            }

            int count = 0;
            int walTTL = WAL_TTL;
            while (it.hasNext()) {
                Pair<Long, WalFileInfo> e = it.next();
                // keep at least two wal
                if (++index < size - 2 && (now - e.other().mtime()) > walTTL) {
                    LOGGER.info("Clean wals, remove " + e.other().path());
                    FileUtils.deleteQuietly(new File(e.other().path()));
                    it.remove();
                    count++;
                }
            }

            if (count > 0) {
                LOGGER.info("Cleaned " + count + " wals");
            }

            firstLogId = walFiles.getFirst().other().firstId();
        } finally {
            walFilesMutex.unlock();
        }
    }

    @Override
    public void cleanWAL(long id) {

    }

    public int accessAllWalInfo(Function<WalFileInfo, Boolean> fn) {
        int count = 0;
        try {
            walFilesMutex.lock();
            for (int i = walFiles.size() - 1; i >= 0; i--) {
                ++count;
                if (!fn.apply(walFiles.get(i).other()))
                    break;
            }
        } finally {
            walFilesMutex.unlock();
        }
        return count;
    }

    interface PreProcessor {
        boolean apply(long logId, long termId, long clusterId, byte[] log);
    }

    public static class FileBasedWalInfo {
        String idStr;
        int spaceId;
        int partId;
    }
}
