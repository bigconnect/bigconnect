package com.mware.ge.kvstore.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class DebugLock extends ReentrantLock {
    private List<String> callers = new ArrayList<>();

    @Override
    public void lock() {
        StackTraceElement caller = new Exception().getStackTrace()[1];
        String callerId = caller.getClassName() + ":" + caller.getMethodName();
        System.out.println("Trylock by "+Thread.currentThread().getName()+": "+callerId+", status: "+getHoldCount());
        super.lock();
        System.out.println("Locked by "+Thread.currentThread().getName()+": "+callerId);
    }

    @Override
    public void unlock() {
        StackTraceElement caller = new Exception().getStackTrace()[1];
        String callerId = caller.getClassName() + ":" + caller.getMethodName();
        super.unlock();
        System.out.println("Unlocked by "+Thread.currentThread().getName()+": "+callerId+", status: "+getHoldCount());
    }
}
