package com.gsb.quota;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Multi-tenant scheduler with per-tenant token-bucket quotas and weighted
 * fair queuing (WFQ) between tenants.
 *
 * <p>Quota: each tenant owns a token bucket that is replenished to exactly
 * {@code tokensPerSecond} tokens at the start of every one-second window
 * (anchored at registration time). At most {@code tokensPerSecond} tasks are
 * released per window; submissions beyond the quota are queued, never
 * dropped.</p>
 *
 * <p>Fairness: among tenants that have queued work and available tokens, the
 * dispatcher always runs the tenant with the smallest virtual finish time.
 * Each dispatch advances the tenant's virtual finish time by
 * {@code SCALE / weight}, so over time completions converge to the weight
 * ratio and a backlogged tenant waits only a bounded number of turns.</p>
 *
 * <p>Concurrency: submissions take only the target tenant's lock; there is no
 * global lock on the submit path. A single dispatcher thread performs
 * selection and runs tasks inline, so tasks should be short.</p>
 *
 * <p>Time: all time reads and all waiting go through the injected
 * {@link Clock}.</p>
 */
public final class Scheduler {

    private static final long WINDOW_MILLIS = 1000L;
    private static final long COST_SCALE = 1L << 20;

    private final Clock clock;
    private final ConcurrentHashMap<String, Tenant> tenants =
            new ConcurrentHashMap<String, Tenant>();
    private final Object idleMonitor = new Object();

    private volatile boolean running;
    private volatile boolean dispatcherWaiting;
    private volatile long globalVirtual;
    private long observedClockVersion;
    private Thread dispatcherThread;

    public Scheduler() {
        this(new SystemClock());
    }

    public Scheduler(Clock clock) {
        if (clock == null) {
            throw new NullPointerException("clock");
        }
        this.clock = clock;
    }

    /**
     * Registers a tenant with a per-second token quota and a scheduling
     * weight. Must be called at most once per tenant id.
     */
    public void register(String tenantId, long tokensPerSecond, int weight) {
        if (tenantId == null) {
            throw new NullPointerException("tenantId");
        }
        if (tokensPerSecond <= 0L) {
            throw new IllegalArgumentException("tokensPerSecond must be > 0");
        }
        if (weight <= 0) {
            throw new IllegalArgumentException("weight must be > 0");
        }
        Tenant tenant = new Tenant(tenantId, tokensPerSecond, weight, clock.nowMillis());
        if (tenants.putIfAbsent(tenantId, tenant) != null) {
            throw new IllegalStateException("tenant already registered: " + tenantId);
        }
        clock.wakeUp();
    }

    /**
     * Enqueues a task for the given tenant and returns its task id. The task
     * is released once the tenant's quota allows it and the weighted-fair
     * scheduler selects the tenant. Never drops the task.
     */
    public String submit(String tenantId, Callable<?> task) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        Tenant tenant = tenants.get(tenantId);
        if (tenant == null) {
            throw new IllegalArgumentException("unknown tenant: " + tenantId);
        }
        String id = UUID.randomUUID().toString();
        synchronized (tenant.lock) {
            if (tenant.queue.isEmpty()) {
                // A tenant rejoining the competition must not hoard virtual
                // time while idle, but it may keep a bounded catch-up credit
                // of one virtual work unit. Clamping all the way up to
                // globalVirtual would equalise tenants that stay backlogged
                // via continuous re-submission and destroy the weight ratio.
                tenant.virtualFinish =
                        Math.max(tenant.virtualFinish, globalVirtual - COST_SCALE);
            }
            tenant.queue.addLast(new Task(id, task));
        }
        clock.wakeUp();
        return id;
    }

    /** Starts the dispatcher thread. */
    public synchronized void start() {
        if (running) {
            throw new IllegalStateException("scheduler already started");
        }
        running = true;
        dispatcherThread = new Thread(new Runnable() {
            public void run() {
                dispatchLoop();
            }
        }, "gsb-quota-dispatcher");
        dispatcherThread.setDaemon(true);
        dispatcherThread.start();
    }

    /** Stops the dispatcher thread and waits for it to finish. */
    public void shutdown() {
        synchronized (this) {
            if (!running) {
                return;
            }
            running = false;
        }
        clock.wakeUp();
        Thread thread = dispatcherThread;
        if (thread != null) {
            thread.interrupt();
            boolean interrupted = false;
            for (;;) {
                try {
                    thread.join();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Test support: blocks until the dispatcher has observed the current
     * clock version and found no releasable work (queues empty or every
     * backlogged tenant is out of tokens until the next window).
     */
    void awaitIdle() {
        Thread thread = dispatcherThread;
        if (thread == null) {
            return;
        }
        synchronized (idleMonitor) {
            while (thread.isAlive()) {
                if (dispatcherWaiting && observedClockVersion == clock.version()) {
                    return;
                }
                try {
                    idleMonitor.wait(1L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void dispatchLoop() {
        while (running) {
            long now = clock.nowMillis();
            Tenant selected = null;
            long bestVirtual = Long.MAX_VALUE;
            long deadline = Long.MAX_VALUE;
            for (Tenant tenant : tenants.values()) {
                synchronized (tenant.lock) {
                    tenant.refill(now);
                    if (tenant.queue.isEmpty()) {
                        continue;
                    }
                    if (tenant.tokens <= 0L) {
                        if (tenant.nextRefillAt < deadline) {
                            deadline = tenant.nextRefillAt;
                        }
                        continue;
                    }
                    if (tenant.virtualFinish < bestVirtual) {
                        bestVirtual = tenant.virtualFinish;
                        selected = tenant;
                    }
                }
            }

            Task task = null;
            if (selected != null) {
                synchronized (selected.lock) {
                    task = selected.queue.pollFirst();
                    selected.tokens -= 1L;
                    selected.virtualFinish += COST_SCALE / selected.weight;
                    if (selected.virtualFinish > globalVirtual) {
                        globalVirtual = selected.virtualFinish;
                    }
                }
            }

            if (task != null) {
                try {
                    task.callable.call();
                } catch (Throwable ignored) {
                    // A failing task must not take the dispatcher down.
                }
                continue;
            }

            // Nothing releasable right now: sleep until the next token
            // replenishment, or until a submission/registration wakes us.
            synchronized (idleMonitor) {
                observedClockVersion = clock.version();
                dispatcherWaiting = true;
                idleMonitor.notifyAll();
            }
            clock.sleepUntil(deadline);
            synchronized (idleMonitor) {
                dispatcherWaiting = false;
            }
        }
    }

    private static final class Task {
        final String id;
        final Callable<?> callable;

        Task(String id, Callable<?> callable) {
            this.id = id;
            this.callable = callable;
        }
    }

    private static final class Tenant {
        final Object lock = new Object();
        final String id;
        final long capacity;
        final int weight;
        final Deque<Task> queue = new ArrayDeque<Task>();
        long tokens;
        long nextRefillAt;
        long virtualFinish;

        Tenant(String id, long tokensPerSecond, int weight, long nowMillis) {
            this.id = id;
            this.capacity = tokensPerSecond;
            this.weight = weight;
            this.tokens = tokensPerSecond;
            this.nextRefillAt = nowMillis + WINDOW_MILLIS;
        }

        /** Replenishes the bucket to full at every window boundary. */
        void refill(long now) {
            if (now >= nextRefillAt) {
                tokens = capacity;
                long elapsed = now - nextRefillAt;
                nextRefillAt += WINDOW_MILLIS * (elapsed / WINDOW_MILLIS + 1L);
            }
        }
    }
}
