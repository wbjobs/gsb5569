package com.gsb.quota;

/**
 * Test clock: time only moves when {@link #advance(long)} or
 * {@link #advanceTo(long)} is called. Threads blocked in
 * {@link #sleepUntil(long)} wake up when time reaches their deadline or when
 * {@link #wakeUp()} is called.
 */
public final class ManualClock implements Clock {

    private long now;
    private long version;
    private boolean wakeupPending;

    public synchronized long nowMillis() {
        return now;
    }

    public synchronized long version() {
        return version;
    }

    public synchronized void advance(long deltaMillis) {
        if (deltaMillis < 0L) {
            throw new IllegalArgumentException("deltaMillis must be >= 0");
        }
        now += deltaMillis;
        version++;
        wakeupPending = true;
        notifyAll();
    }

    public synchronized void advanceTo(long targetMillis) {
        if (targetMillis > now) {
            advance(targetMillis - now);
        }
    }

    public synchronized void wakeUp() {
        wakeupPending = true;
        notifyAll();
    }

    public void sleepUntil(long deadlineMillis) {
        synchronized (this) {
            while (!wakeupPending && now < deadlineMillis) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            wakeupPending = false;
        }
    }
}
