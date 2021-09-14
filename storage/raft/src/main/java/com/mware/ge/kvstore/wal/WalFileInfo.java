package com.mware.ge.kvstore.wal;

public class WalFileInfo {
    private String fullpath;
    private final long firstLogId;
    private long lastLogId;
    private long lastLogTerm;
    private long mtime;
    private long size;

    public WalFileInfo(String path, long firstId) {
        this.fullpath = path;
        this.firstLogId = firstId;
        this.lastLogId = -1;
        this.lastLogTerm = -1;
        this.mtime = 0;
        this.size = 0;
    }

    public String path() {
        return fullpath;
    }

    public long firstId() {
        return firstLogId;
    }

    public long lastId() {
        return lastLogId;
    }

    public void setLastId(long id) {
        this.lastLogId = id;
    }

    public long lastTerm() {
        return lastLogTerm;
    }

    public void setLastTerm(long term) {
        this.lastLogTerm = term;
    }

    public long mtime() {
        return mtime;
    }

    public void setMtime(long time) {
        this.mtime = time;
    }

    public long size() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
}
