package com.mware.ge.metric;

public interface Clock {
    long getTick();

    static Clock defaultClock() {
        return UserTimeClockHolder.DEFAULT;
    }

    class UserTimeClock implements Clock {
        @Override
        public long getTick() {
            return System.nanoTime();
        }
    }

    class UserTimeClockHolder {
        private static final Clock DEFAULT = new UserTimeClock();
    }
}
