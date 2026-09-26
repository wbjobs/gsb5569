package com.gsb.quota;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Weighted fairness: three tenants with weights 1:2:3 and quotas that never
 * bind. Every task performs one millisecond of virtual work (by advancing the
 * manual clock) and re-submits itself, so every tenant stays backlogged for
 * the whole run. After 30 seconds of virtual time the completion counts must
 * match the 1:2:3 ratio within 15%, and no tenant may go more than one
 * second of virtual time without a completion.
 */
final class FairnessTest implements TestCase {

    private static final long HORIZON_MS = 30000L;
    private static final long STARVATION_LIMIT_MS = 1000L;
    private static final double TOLERANCE = 0.15;

    public String name() {
        return "fairness-weights-1:2:3";
    }

    public void run() {
        final ManualClock clock = new ManualClock();
        final Scheduler scheduler = new Scheduler(clock);
        long hugeQuota = 1000000000L;
        scheduler.register("tenant-w1", hugeQuota, 1);
        scheduler.register("tenant-w2", hugeQuota, 2);
        scheduler.register("tenant-w3", hugeQuota, 3);

        final List<Long> completions1 = new ArrayList<Long>();
        final List<Long> completions2 = new ArrayList<Long>();
        final List<Long> completions3 = new ArrayList<Long>();

        scheduler.submit("tenant-w1", new RepeatingTask(scheduler, clock, "tenant-w1", completions1));
        scheduler.submit("tenant-w2", new RepeatingTask(scheduler, clock, "tenant-w2", completions2));
        scheduler.submit("tenant-w3", new RepeatingTask(scheduler, clock, "tenant-w3", completions3));

        scheduler.start();

        // Wait until 30 seconds of virtual time have elapsed. Each task
        // advances the clock by 1 ms, so this is ~30000 dispatches.
        synchronized (clock) {
            while (clock.nowMillis() < HORIZON_MS) {
                try {
                    clock.wait(100L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while waiting for virtual horizon");
                }
            }
        }
        scheduler.shutdown();

        long c1 = completions1.size();
        long c2 = completions2.size();
        long c3 = completions3.size();
        long total = c1 + c2 + c3;
        Check.isTrue(total >= HORIZON_MS,
                "expected at least " + HORIZON_MS + " completions, got " + total);

        assertShare("tenant-w1", c1, total, 1);
        assertShare("tenant-w2", c2, total, 2);
        assertShare("tenant-w3", c3, total, 3);

        assertNoStarvation("tenant-w1", completions1);
        assertNoStarvation("tenant-w2", completions2);
        assertNoStarvation("tenant-w3", completions3);
    }

    private static void assertShare(String tenant, long count, long total, int weight) {
        double share = (double) count / (double) total;
        double expected = (double) weight / 6.0;
        Check.isTrue(Math.abs(share - expected) <= TOLERANCE * expected,
                tenant + " share " + share + " deviates from expected " + expected
                        + " by more than " + (TOLERANCE * 100.0) + "% (count=" + count
                        + ", total=" + total + ")");
    }

    private static void assertNoStarvation(String tenant, List<Long> completions) {
        Check.isTrue(!completions.isEmpty(), tenant + " completed nothing");
        long previous = 0L;
        long maxGap = 0L;
        for (int i = 0; i < completions.size(); i++) {
            long at = completions.get(i).longValue();
            maxGap = Math.max(maxGap, at - previous);
            previous = at;
        }
        Check.isTrue(maxGap <= STARVATION_LIMIT_MS,
                tenant + " starved for " + maxGap + " ms of virtual time (limit "
                        + STARVATION_LIMIT_MS + " ms)");
    }

    /**
     * A task that models 1 ms of work and keeps its tenant backlogged by
     * re-submitting itself.
     */
    private static final class RepeatingTask implements Callable<Object> {
        private final Scheduler scheduler;
        private final ManualClock clock;
        private final String tenantId;
        private final List<Long> completions;

        RepeatingTask(Scheduler scheduler, ManualClock clock, String tenantId, List<Long> completions) {
            this.scheduler = scheduler;
            this.clock = clock;
            this.tenantId = tenantId;
            this.completions = completions;
        }

        public Object call() {
            completions.add(Long.valueOf(clock.nowMillis()));
            clock.advance(1L);
            scheduler.submit(tenantId, this);
            return null;
        }
    }
}
