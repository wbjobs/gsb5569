package com.gsb.quota;

/**
 * Time source for the scheduler. All waiting and all time reads inside the
 * scheduler go through this interface so tests can drive virtual time.
 */
public interface Clock {

    /** Current time in milliseconds. */
    long nowMillis();

    /**
     * Blocks the calling thread until the clock reaches {@code deadlineMillis}
     * or {@link #wakeUp()} is called. Must also return when the thread is
     * interrupted.
     */
    void sleepUntil(long deadlineMillis);

    /** Wakes any thread blocked in {@link #sleepUntil(long)}. */
    void wakeUp();

    /**
     * Monotonic version of the time source, bumped on every manual advance.
     * Used by tests to know when the dispatcher has observed the latest time.
     * Real-time implementations may return a constant.
     */
    long version();
}
