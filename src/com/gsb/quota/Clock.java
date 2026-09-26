package com.gsb.quota;

/**
 * Injectable time source. All scheduler timing decisions read time exclusively
 * through this interface, so tests can drive the scheduler with virtual time.
 */
public interface Clock {

    /** Current time in nanoseconds, monotonically non-decreasing. */
    long nanoTime();

    /**
     * Registers a one-shot callback to be invoked when the clock reaches
     * {@code deadlineNanos}. If the deadline has already passed, the callback
     * may be invoked synchronously before this method returns.
     *
     * @return an opaque handle that can be passed to {@link #cancelWakeup(Object)}
     */
    Object scheduleWakeup(long deadlineNanos, Runnable callback);

    /** Cancels a previously scheduled wakeup. Cancelling twice is harmless. */
    void cancelWakeup(Object handle);
}
