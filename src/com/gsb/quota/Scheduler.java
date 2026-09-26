package com.gsb.quota;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Multi-tenant scheduler with per-tenant token-bucket quotas and weighted-fair
 * dispatching.
 *
 * Fairness model: the scheduler itself is the scarce resource, dispatching at
 * most {@code maxTasksPerSecond} tasks in total. Among tenants that have queued
 * work and a quota token available, the tenant with the smallest virtual finish
 * time ({@code virtualTime + 1/weight}) is served next, and its virtual time
 * advances by {@code 1/weight} virtual seconds per task. This is classic
 * weighted fair queueing: long-run completion shares converge to
 * {@code weight / sum(weights)}, and a backlogged tenant's virtual time never
 * lags the most advanced tenant by more than {@code 1/weight} virtual seconds
 * (i.e. at most one virtual second for any weight >= 1).
 *
 * Quota model: each tenant owns a token bucket with capacity of exactly one
 * token, refilling at {@code tokensPerSecond}. Because releases are therefore
 * spaced by at least {@code ceil(1e9 / tokensPerSecond)} nanoseconds, no
 * one-second window can ever contain more than {@code tokensPerSecond}
 * releases, and at most one token can be banked while idle. Tasks submitted
 * beyond the quota are queued, never dropped.
 *
 * All timing goes through the injected {@link Clock}; this class contains no
 * {@code System.currentTimeMillis} and no {@code Thread.sleep}.
 */
public class Scheduler {

    private static final long SECOND_NANOS = 1000000000L;
    private static final double EPSILON = 1e-9;
    private static final int WORKER_THREADS = 4;

    private static final class PendingTask {
        final String id;
        final TenantState tenant;
        final Callable<?> task;

        PendingTask(String id, TenantState tenant, Callable<?> task) {
            this.id = id;
            this.tenant = tenant;
            this.task = task;
        }
    }

    private static final class TenantState {
        final String id;
        final int weight;
        final long tokenIntervalNanos;
        long nextTokenNanos;
        double virtualTime;
        double maxVirtualLag;
        long completed;
        final Deque<PendingTask> queue = new ArrayDeque<PendingTask>();

        TenantState(String id, long tokensPerSecond, int weight) {
            this.id = id;
            this.weight = weight;
            this.tokenIntervalNanos = (SECOND_NANOS + tokensPerSecond - 1) / tokensPerSecond;
        }
    }

    private final Clock clock;
    private final long globalCostNanos;
    private final Object lock = new Object();
    private final Map<String, TenantState> tenants = new HashMap<String, TenantState>();

    private final Runnable wakeupCallback = new Runnable() {
        public void run() {
            synchronized (lock) {
                generation++;
                lock.notifyAll();
            }
        }
    };

    // Guarded by lock.
    private long globalAllowanceNanos;
    private long globalLastAccrualNanos;
    private double maxVirtualTime;
    private ExecutorService executor;
    private Thread dispatcherThread;
    private boolean started;
    private boolean running;
    private boolean shutdown;
    private long generation;
    private long waitingGeneration;
    private int inFlight;
    private boolean dispatcherWaiting;
    private long taskSequence;

    public Scheduler(Clock clock) {
        this(clock, 1000);
    }

    public Scheduler(Clock clock, long maxTasksPerSecond) {
        if (clock == null) {
            throw new NullPointerException("clock");
        }
        if (maxTasksPerSecond < 1) {
            throw new IllegalArgumentException("maxTasksPerSecond must be >= 1");
        }
        this.clock = clock;
        this.globalCostNanos = (SECOND_NANOS + maxTasksPerSecond - 1) / maxTasksPerSecond;
    }

    public void register(String tenantId, long tokensPerSecond, int weight) {
        synchronized (lock) {
            if (shutdown) {
                throw new IllegalStateException("scheduler is shut down");
            }
            if (tenantId == null || tenantId.length() == 0) {
                throw new IllegalArgumentException("tenantId must not be empty");
            }
            if (tokensPerSecond < 1) {
                throw new IllegalArgumentException("tokensPerSecond must be >= 1");
            }
            if (weight < 1) {
                throw new IllegalArgumentException("weight must be >= 1");
            }
            if (tenants.containsKey(tenantId)) {
                throw new IllegalArgumentException("tenant already registered: " + tenantId);
            }
            TenantState tenant = new TenantState(tenantId, tokensPerSecond, weight);
            // Bucket starts with one token available (capacity is one token).
            tenant.nextTokenNanos = clock.nanoTime();
            tenants.put(tenantId, tenant);
            generation++;
            lock.notifyAll();
        }
    }

    public String submit(String tenantId, Callable task) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        synchronized (lock) {
            if (shutdown) {
                throw new IllegalStateException("scheduler is shut down");
            }
            TenantState tenant = tenants.get(tenantId);
            if (tenant == null) {
                throw new IllegalArgumentException("unknown tenant: " + tenantId);
            }
            String id = "task-" + (++taskSequence);
            tenant.queue.addLast(new PendingTask(id, tenant, task));
            generation++;
            lock.notifyAll();
            return id;
        }
    }

    public void start() {
        synchronized (lock) {
            if (shutdown) {
                throw new IllegalStateException("scheduler is shut down");
            }
            if (started) {
                return;
            }
            started = true;
            running = true;
            // The global bucket starts with one token, like the tenant buckets.
            globalAllowanceNanos = globalCostNanos;
            globalLastAccrualNanos = clock.nanoTime();
            executor = Executors.newFixedThreadPool(WORKER_THREADS, new ThreadFactory() {
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "gsb-quota-worker");
                    thread.setDaemon(true);
                    return thread;
                }
            });
            dispatcherThread = new Thread(new Runnable() {
                public void run() {
                    dispatchLoop();
                }
            }, "gsb-quota-dispatcher");
            dispatcherThread.setDaemon(true);
            dispatcherThread.start();
        }
    }

    public void shutdown() {
        Thread dispatcher;
        ExecutorService exec;
        synchronized (lock) {
            if (shutdown) {
                return;
            }
            shutdown = true;
            running = false;
            generation++;
            lock.notifyAll();
            dispatcher = dispatcherThread;
            exec = executor;
        }
        if (dispatcher != null) {
            boolean joined = false;
            while (!joined) {
                try {
                    dispatcher.join();
                    joined = true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        if (exec != null) {
            exec.shutdown();
            boolean terminated = false;
            while (!terminated) {
                try {
                    terminated = exec.awaitTermination(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** Number of tasks of the given tenant that have finished executing. */
    public long completedCount(String tenantId) {
        synchronized (lock) {
            TenantState tenant = tenants.get(tenantId);
            return tenant == null ? 0 : tenant.completed;
        }
    }

    /** Number of tasks of the given tenant still waiting in the quota queue. */
    public int queuedCount(String tenantId) {
        synchronized (lock) {
            TenantState tenant = tenants.get(tenantId);
            return tenant == null ? 0 : tenant.queue.size();
        }
    }

    /**
     * Largest observed virtual-time lag (in virtual seconds) of the tenant
     * behind the most advanced tenant while it had work queued. Weighted fair
     * queueing bounds this by {@code 1/weight}, i.e. one virtual second at most.
     */
    public double maxVirtualLag(String tenantId) {
        synchronized (lock) {
            TenantState tenant = tenants.get(tenantId);
            return tenant == null ? 0 : tenant.maxVirtualLag;
        }
    }

    /**
     * Test hook: blocks (up to {@code timeoutMillis}) until the dispatcher has
     * nothing left to dispatch and every dispatched task has finished.
     */
    boolean awaitIdle(long timeoutMillis) throws InterruptedException {
        synchronized (lock) {
            long deadline = System.nanoTime() + timeoutMillis * 1000000L;
            for (;;) {
                if (dispatcherWaiting && inFlight == 0 && waitingGeneration == generation) {
                    return true;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                lock.wait(remaining / 1000000L, (int) (remaining % 1000000L));
            }
        }
    }

    private void dispatchLoop() {
        synchronized (lock) {
            while (running) {
                long now = clock.nanoTime();
                accrueGlobalLocked(now);
                TenantState picked;
                while (running && (picked = pickEligibleLocked(now)) != null) {
                    dispatchLocked(picked, now);
                    now = clock.nanoTime();
                    accrueGlobalLocked(now);
                }
                if (!running) {
                break;
            }
            long deadline = nextDeadlineLocked(now);
            // Snapshot the generation before arming the wakeup: if the deadline
            // already passed, the callback fires synchronously and bumps the
            // generation, which must not be mistaken for a consumed wakeup.
            long observedGeneration = generation;
            Object handle = null;
            if (deadline != Long.MAX_VALUE) {
                handle = clock.scheduleWakeup(deadline, wakeupCallback);
            }
            if (generation == observedGeneration && running) {
                dispatcherWaiting = true;
                waitingGeneration = observedGeneration;
                lock.notifyAll();
                try {
                    while (generation == observedGeneration && running) {
                        lock.wait();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    dispatcherWaiting = false;
                    return;
                }
                dispatcherWaiting = false;
            }
            if (handle != null) {
                clock.cancelWakeup(handle);
            }
        }
    }
    }

    private void accrueGlobalLocked(long now) {
        long elapsed = now - globalLastAccrualNanos;
        if (elapsed > 0) {
            globalAllowanceNanos = Math.min(SECOND_NANOS, globalAllowanceNanos + elapsed);
            globalLastAccrualNanos = now;
        }
    }

    private TenantState pickEligibleLocked(long now) {
        if (globalAllowanceNanos < globalCostNanos) {
            return null;
        }
        TenantState best = null;
        double bestFinish = 0;
        for (TenantState tenant : tenants.values()) {
            if (tenant.queue.isEmpty() || now < tenant.nextTokenNanos) {
                continue;
            }
            double finish = tenant.virtualTime + 1.0 / tenant.weight;
            if (best == null
                    || finish < bestFinish - EPSILON
                    || (Math.abs(finish - bestFinish) <= EPSILON && tenant.id.compareTo(best.id) < 0)) {
                best = tenant;
                bestFinish = finish;
            }
        }
        return best;
    }

    private void dispatchLocked(TenantState tenant, long now) {
        final PendingTask pending = tenant.queue.pollFirst();
        tenant.nextTokenNanos = Math.max(tenant.nextTokenNanos, now) + tenant.tokenIntervalNanos;
        globalAllowanceNanos -= globalCostNanos;
        tenant.virtualTime += 1.0 / tenant.weight;
        if (tenant.virtualTime > maxVirtualTime) {
            maxVirtualTime = tenant.virtualTime;
        }
        for (TenantState other : tenants.values()) {
            if (other.queue.isEmpty()) {
                continue;
            }
            double lag = maxVirtualTime - other.virtualTime;
            if (lag > other.maxVirtualLag) {
                other.maxVirtualLag = lag;
            }
        }
        inFlight++;
        executor.execute(new Runnable() {
            public void run() {
                try {
                    pending.task.call();
                } catch (Throwable ignored) {
                    // A failing task still counts as completed work for the tenant.
                } finally {
                    synchronized (lock) {
                        pending.tenant.completed++;
                        inFlight--;
                        lock.notifyAll();
                    }
                }
            }
        });
    }

    private long nextDeadlineLocked(long now) {
        long deadline = Long.MAX_VALUE;
        boolean anyQueued = false;
        for (TenantState tenant : tenants.values()) {
            if (tenant.queue.isEmpty()) {
                continue;
            }
            anyQueued = true;
            // Only tenants still waiting for a token contribute a deadline;
            // a tenant whose token is already available (nextTokenNanos <= now)
            // is blocked solely on the global allowance, handled below.
            if (tenant.nextTokenNanos > now && tenant.nextTokenNanos < deadline) {
                deadline = tenant.nextTokenNanos;
            }
        }
        if (!anyQueued) {
            return Long.MAX_VALUE;
        }
        if (globalAllowanceNanos < globalCostNanos) {
            long globalReady = now + (globalCostNanos - globalAllowanceNanos);
            if (globalReady < deadline) {
                deadline = globalReady;
            }
        }
        return deadline;
    }
}
