package com.gsb.quota;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Test clock: time only moves when {@link #advanceBy(long)} or
 * {@link #advanceTo(long)} is called. Due wakeups fire synchronously on the
 * advancing thread (outside the clock monitor, so callbacks may re-enter the
 * scheduler without deadlocking).
 */
public class ManualClock implements Clock {

    private static final class Wakeup {
        final long deadlineNanos;
        final Runnable callback;
        boolean cancelled;

        Wakeup(long deadlineNanos, Runnable callback) {
            this.deadlineNanos = deadlineNanos;
            this.callback = callback;
        }
    }

    private long nowNanos;
    private final List<Wakeup> wakeups = new ArrayList<Wakeup>();

    public synchronized long nanoTime() {
        return nowNanos;
    }

    public Object scheduleWakeup(long deadlineNanos, Runnable callback) {
        Wakeup wakeup = new Wakeup(deadlineNanos, callback);
        boolean fireNow;
        synchronized (this) {
            fireNow = deadlineNanos <= nowNanos;
            if (!fireNow) {
                wakeups.add(wakeup);
            }
        }
        if (fireNow) {
            callback.run();
        }
        return wakeup;
    }

    public void cancelWakeup(Object handle) {
        synchronized (this) {
            ((Wakeup) handle).cancelled = true;
        }
    }

    public void advanceBy(long nanos) {
        advanceTo(nanoTime() + nanos);
    }

    public void advanceTo(long targetNanos) {
        List<Wakeup> due = new ArrayList<Wakeup>();
        synchronized (this) {
            if (targetNanos < nowNanos) {
                throw new IllegalArgumentException("clock cannot move backwards");
            }
            nowNanos = targetNanos;
            Iterator<Wakeup> it = wakeups.iterator();
            while (it.hasNext()) {
                Wakeup wakeup = it.next();
                if (wakeup.cancelled) {
                    it.remove();
                    continue;
                }
                if (wakeup.deadlineNanos <= nowNanos) {
                    due.add(wakeup);
                    it.remove();
                }
            }
        }
        for (Wakeup wakeup : due) {
            wakeup.callback.run();
        }
    }
}
