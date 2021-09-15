package com.mware.ge.kvstore.wal;

public class FileBasedWalPolicy {
    // The maximum size of each log message file (in byte). When the existing
    // log file reaches this size, a new file will be created
    long fileSize = 16 * 1024L * 1024L;

    // Size of each buffer (in byte)
    int bufferSize = 8 * 1024 * 1024;

    // Whether fsync needs to be called every write
    boolean sync = false;

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }
}
