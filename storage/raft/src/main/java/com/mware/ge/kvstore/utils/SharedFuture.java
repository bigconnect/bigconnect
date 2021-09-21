package com.mware.ge.kvstore.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SharedFuture<T> extends CompletableFuture<T> {
    Lock lock = new ReentrantLock();
    List<CompletableFuture<T>> futures = new ArrayList<>();

    public SharedFuture() {
        super();
    }

    public CompletableFuture<T> getFuture() {
        try {
            lock.lock();
            CompletableFuture<T> future = new CompletableFuture<>();
            futures.add(future);
            return future;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean complete(T value) {
        try {
            lock.lock();
            boolean complete = super.complete(value);

            for (CompletableFuture<T> f : futures) {
                f.complete(value);
            }

            return complete;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        try {
            lock.lock();
            return futures.size();
        } finally {
            lock.unlock();
        }
    }
}
