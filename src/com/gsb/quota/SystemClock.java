package com.gsb.quota;

/**
 * Production clock backed by {@link System#nanoTime()}.
 */
public final class SystemClock implements Clock {

    private final Object monitor = new Object();
    private boolean wakeupPending;

    public long nowMillis() {
        return System.nanoTime() / 1000000L;
    }

    public long version() {
        return 0L;
    }

    public void sleepUntil(long deadlineMillis) {
        synchronized (monitor) {
            for (;;) {
                if (wakeupPending) {
                    wakeupPending = false;
                    return;
                }
                long remaining = deadlineMillis - nowMillis();
                if (remaining <= 0L) {
                    return;
                }
                try {
                    monitor.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    public void wakeUp() {
        synchronized (monitor) {
            wakeupPending = true;
            monitor.notifyAll();
        }
    }
}
