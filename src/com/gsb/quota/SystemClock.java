package com.gsb.quota;

import java.util.PriorityQueue;

/**
 * Production clock backed by {@link System#nanoTime()}. Wakeups are delivered
 * by a single daemon timer thread using monitor waits (no Thread.sleep).
 */
public class SystemClock implements Clock {

    private static final class Wakeup implements Comparable<Wakeup> {
        final long deadlineNanos;
        final Runnable callback;
        boolean cancelled;

        Wakeup(long deadlineNanos, Runnable callback) {
            this.deadlineNanos = deadlineNanos;
            this.callback = callback;
        }

        public int compareTo(Wakeup other) {
            return Long.compare(deadlineNanos, other.deadlineNanos);
        }
    }

    private final Object monitor = new Object();
    private final PriorityQueue<Wakeup> queue = new PriorityQueue<Wakeup>();
    private final Thread timerThread;

    public SystemClock() {
        timerThread = new Thread(new Runnable() {
            public void run() {
                timerLoop();
            }
        }, "gsb-quota-clock");
        timerThread.setDaemon(true);
        timerThread.start();
    }

    public long nanoTime() {
        return System.nanoTime();
    }

    public Object scheduleWakeup(long deadlineNanos, Runnable callback) {
        Wakeup wakeup = new Wakeup(deadlineNanos, callback);
        synchronized (monitor) {
            queue.add(wakeup);
            monitor.notifyAll();
        }
        return wakeup;
    }

    public void cancelWakeup(Object handle) {
        synchronized (monitor) {
            ((Wakeup) handle).cancelled = true;
            monitor.notifyAll();
        }
    }

    private void timerLoop() {
        for (;;) {
            Wakeup due = null;
            synchronized (monitor) {
                boolean wasInterrupted = false;
                for (;;) {
                    while (!queue.isEmpty() && queue.peek().cancelled) {
                        queue.poll();
                    }
                    if (queue.isEmpty()) {
                        try {
                            monitor.wait();
                        } catch (InterruptedException e) {
                            wasInterrupted = true;
                        }
                        continue;
                    }
                    long delay = queue.peek().deadlineNanos - System.nanoTime();
                    if (delay <= 0) {
                        due = queue.poll();
                        break;
                    }
                    long millis = delay / 1000000L;
                    int nanos = (int) (delay - millis * 1000000L);
                    try {
                        monitor.wait(millis, nanos);
                    } catch (InterruptedException e) {
                        wasInterrupted = true;
                    }
                }
                if (wasInterrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            if (due != null && !due.cancelled) {
                due.callback.run();
            }
        }
    }
}
