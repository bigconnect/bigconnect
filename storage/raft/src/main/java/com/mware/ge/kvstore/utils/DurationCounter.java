package com.mware.ge.kvstore.utils;

public class DurationCounter {
    boolean isPaused = false;
    long accumulated;
    long startTick;

    public DurationCounter() {
        this(false);
    }

    public DurationCounter(boolean paused) {
        reset(paused);
    }

    public void reset(boolean paused) {
        isPaused = paused;
        accumulated = 0;
        if (isPaused) {
            startTick = 0;
        } else {
            startTick = System.nanoTime();
        }
    }

    public void pause() {
        if (isPaused) {
            return;
        }

        isPaused = true;
        accumulated += (System.nanoTime() - startTick);
        startTick = 0;
    }

    public void resume() {
        if (!isPaused) {
            return;
        }

        startTick = System.nanoTime();
        isPaused = false;
    }

    public long elapsedInSec() {
        long ticks = isPaused ? accumulated : System.nanoTime() - startTick + accumulated;
        return Math.round(ticks / 1_000_000_000.0 + 0.5);
    }

    public long elapsedInMSec() {
        long ticks = isPaused ? accumulated : System.nanoTime() - startTick + accumulated;
        return Math.round(ticks / 1_000_000.0 + 0.5);
    }

    public long elapsedInUSec() {
        long ticks = isPaused ? accumulated : System.nanoTime() - startTick + accumulated;
        return Math.round(ticks / 1_000.0 + 0.5);
    }

    public boolean isPaused() {
        return isPaused;
    }
}
