package com.mware.ge.kvstore.raftex;

import com.mware.ge.util.Preconditions;

import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;

public class PromiseSet<ValueType> {
    // Whether the last future was returned from a shared promise
    boolean rollSharedPromise_ = true;

    // Promises shared by continuous non atomic op logs
    LinkedList<CompletableFuture<ValueType>> sharedPromises_ = new LinkedList<>();

    // A list of promises for atomic op logs
    LinkedList<CompletableFuture<ValueType>> singlePromises_ = new LinkedList<>();

    public CompletableFuture<ValueType> getSharedFuture() {
        if (rollSharedPromise_) {
            sharedPromises_.add(new CompletableFuture<>());
            rollSharedPromise_ = false;
        }
        return sharedPromises_.getLast();
    }

    public CompletableFuture<ValueType> getSingleFuture() {
        singlePromises_.add(new CompletableFuture<>());
        rollSharedPromise_ = true;
        return singlePromises_.getLast();
    }

    public CompletableFuture<ValueType> getAndRollSharedFuture() {
        if (rollSharedPromise_) {
            sharedPromises_.add(new CompletableFuture<>());
        }
        rollSharedPromise_ = true;
        return sharedPromises_.getLast();
    }

    public void setOneSharedValue(ValueType val) {
        Preconditions.checkState(!sharedPromises_.isEmpty());
        sharedPromises_.getFirst().complete(val);
        sharedPromises_.pop();
    }

    public void setOneSingleValue(ValueType val) {
        Preconditions.checkState(!singlePromises_.isEmpty());
        singlePromises_.getFirst().complete(val);
        singlePromises_.pop();
    }

    public void setValue(ValueType val) {
        for (CompletableFuture<ValueType> p : sharedPromises_) {
            p.complete(val);
        }
        for (CompletableFuture<ValueType> p : singlePromises_) {
            p.complete(val);
        }
    }

    public void reset() {
        sharedPromises_.clear();
        singlePromises_.clear();
        rollSharedPromise_ = true;
    }

    public PromiseSet<ValueType> copy() {
        PromiseSet<ValueType> inst = new PromiseSet<>();
        inst.sharedPromises_ = new LinkedList<>(sharedPromises_);
        inst.singlePromises_ = new LinkedList<>(singlePromises_);
        inst.rollSharedPromise_ = rollSharedPromise_;
        return inst;
    }
}
