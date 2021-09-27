package com.mware.ge.kvstore.raftex;

public class Status {
    public static final int Ok = 0;
    public static final int Error = 101;

    int code;
    String msg;

    public Status(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public static Status OK() {
        return new Status(Ok, null);
    }

    public boolean ok() {
        return code == Ok;
    }
}
