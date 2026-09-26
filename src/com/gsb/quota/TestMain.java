package com.gsb.quota;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Self-contained test suite (no JUnit). Every timing-sensitive test drives the
 * scheduler with a ManualClock, so the whole suite finishes in well under a
 * second of real time regardless of how much virtual time is simulated.
 */
public class TestMain {

    private static final long SECOND = 1000000000L;
    private static final long STEP_200MS = SECOND / 5;

    private static int passed;
    private static int failed;

    private static final Callable<Object> NOOP = new Callable<Object>() {
        public Object call() {
            return null;
        }
    };

    private interface TestBody {
        void run() throws Exception;
    }

    public static void main(String[] args) throws Exception {
        run("weighted fairness 1:2:3 ratio and starvation bound", new TestBody() {
            public void run() throws Exception {
                testWeightedFairness();
            }
        });
        run("per-second release window never exceeds quota and nothing is dropped", new TestBody() {
            public void run() throws Exception {
                testQuotaWindowAndNoDrop();
            }
        });
        run("token bucket does not release one extra at the exact quota boundary", new TestBody() {
            public void run() throws Exception {
                testQuotaBoundaryExact();
            }
        });
        run("idle tenant banks at most one token", new TestBody() {
            public void run() throws Exception {
                testIdleBankingCappedAtOne();
            }
        });
        run("argument validation and lifecycle", new TestBody() {
            public void run() throws Exception {
                testValidationAndLifecycle();
            }
        });
        run("system clock smoke test", new TestBody() {
            public void run() throws Exception {
                testSystemClockSmoke();
            }
        });

        System.out.println("----------------------------------------");
        System.out.println("passed: " + passed + ", failed: " + failed);
        System.exit(failed == 0 ? 0 : 1);
    }

    private static void run(String name, TestBody body) {
        try {
            body.run();
            passed++;
            System.out.println("PASS  " + name);
        } catch (Throwable t) {
            failed++;
            System.out.println("FAIL  " + name + "  ->  " + t);
            t.printStackTrace(System.out);
        }
    }

    /**
     * Three tenants with weights 1:2:3, all permanently backlogged, share a
     * scheduler limited to 600 dispatches/second. After 10 virtual seconds the
     * completion counts must be within 15% of the 1:2:3 target ratio, and no
     * tenant may have starved for more than one virtual second.
     */
    private static void testWeightedFairness() throws Exception {
        ManualClock clock = new ManualClock();
        Scheduler scheduler = new Scheduler(clock, 600);
        scheduler.register("A", 100000, 1);
        scheduler.register("B", 100000, 2);
        scheduler.register("C", 100000, 3);
        scheduler.start();
        for (int i = 0; i < 3000; i++) {
            scheduler.submit("A", NOOP);
            scheduler.submit("B", NOOP);
            scheduler.submit("C", NOOP);
        }
        // 10 virtual seconds in 2ms steps; the scheduler idles between steps.
        for (int i = 0; i < 5000; i++) {
            clock.advanceBy(2 * 1000 * 1000L);
            check(scheduler.awaitIdle(10000), "scheduler did not go idle during fairness run");
        }
        long a = scheduler.completedCount("A");
        long b = scheduler.completedCount("B");
        long c = scheduler.completedCount("C");
        long total = a + b + c;
        check(total >= 5900 && total <= 6100, "expected ~6000 completions in 10s, got " + total);
        assertShare("A", a, total, 1, 6);
        assertShare("B", b, total, 2, 6);
        assertShare("C", c, total, 3, 6);
        check(scheduler.maxVirtualLag("A") <= 1.0 + 1e-6, "A starved: lag=" + scheduler.maxVirtualLag("A"));
        check(scheduler.maxVirtualLag("B") <= 1.0 + 1e-6, "B starved: lag=" + scheduler.maxVirtualLag("B"));
        check(scheduler.maxVirtualLag("C") <= 1.0 + 1e-6, "C starved: lag=" + scheduler.maxVirtualLag("C"));
        scheduler.shutdown();
    }

    /**
     * One tenant with a 5 tokens/second quota and 12 queued tasks: releases in
     * any one-second window must stay <= 5, excess tasks must wait in the queue
     * (visible via queuedCount) and every task must eventually complete.
     */
    private static void testQuotaWindowAndNoDrop() throws Exception {
        ManualClock clock = new ManualClock();
        Scheduler scheduler = new Scheduler(clock, 1000000);
        scheduler.register("T", 5, 1);
        scheduler.start();
        List<String> ids = new ArrayList<String>();
        for (int i = 0; i < 12; i++) {
            ids.add(scheduler.submit("T", NOOP));
        }
        check(new HashSet<String>(ids).size() == 12, "task ids must be unique");

        List<Long> times = new ArrayList<Long>();
        List<Long> counts = new ArrayList<Long>();
        check(scheduler.awaitIdle(10000), "idle at t=0");
        times.add(0L);
        counts.add(scheduler.completedCount("T"));
        for (int i = 1; i <= 12; i++) {
            clock.advanceBy(STEP_200MS);
            check(scheduler.awaitIdle(10000), "idle at step " + i);
            times.add(i * STEP_200MS);
            counts.add(scheduler.completedCount("T"));
        }
        check(scheduler.completedCount("T") == 12,
                "all 12 tasks must complete, got " + scheduler.completedCount("T"));
        // Excess submissions were queued, not dropped: at t=0.4s only 3 ran.
        check(counts.get(2) == 3, "expected 3 completions at t=0.4s, got " + counts.get(2));
        check(scheduler.queuedCount("T") == 0, "queue must be drained at the end");
        // Sliding one-second window over the recorded release history.
        for (int i = 0; i < times.size(); i++) {
            for (int j = i + 1; j < times.size(); j++) {
                if (times.get(j) - times.get(i) <= SECOND) {
                    long released = counts.get(j) - counts.get(i);
                    check(released <= 5,
                            "released " + released + " in a 1s window starting at " + times.get(i) + "ns");
                }
            }
        }
        scheduler.shutdown();
    }

    /**
     * Quota 5/second, 6 tasks: at t=1s-epsilon exactly 5 may be released; the
     * 6th token only becomes available at exactly t=1s.
     */
    private static void testQuotaBoundaryExact() throws Exception {
        ManualClock clock = new ManualClock();
        Scheduler scheduler = new Scheduler(clock, 1000000);
        scheduler.register("T", 5, 1);
        scheduler.start();
        for (int i = 0; i < 6; i++) {
            scheduler.submit("T", NOOP);
        }
        for (int i = 0; i < 4; i++) {
            clock.advanceBy(STEP_200MS);
            check(scheduler.awaitIdle(10000), "idle at step " + i);
        }
        check(scheduler.completedCount("T") == 5,
                "expected exactly 5 releases by t=0.8s, got " + scheduler.completedCount("T"));
        clock.advanceTo(SECOND - 1);
        check(scheduler.awaitIdle(10000), "idle just before 1s");
        check(scheduler.completedCount("T") == 5,
                "bucket released one extra before the boundary: " + scheduler.completedCount("T"));
        clock.advanceBy(1);
        check(scheduler.awaitIdle(10000), "idle at 1s");
        check(scheduler.completedCount("T") == 6,
                "6th token must be available at exactly t=1s, got " + scheduler.completedCount("T"));
        scheduler.shutdown();
    }

    /**
     * A tenant idle for 5 virtual seconds may bank at most one token, so a
     * burst of 7 tasks still respects 5 releases per one-second window.
     */
    private static void testIdleBankingCappedAtOne() throws Exception {
        ManualClock clock = new ManualClock();
        Scheduler scheduler = new Scheduler(clock, 1000000);
        scheduler.register("T", 5, 1);
        scheduler.start();
        clock.advanceBy(5 * SECOND);
        for (int i = 0; i < 7; i++) {
            scheduler.submit("T", NOOP);
        }
        check(scheduler.awaitIdle(10000), "idle after burst submit");
        check(scheduler.completedCount("T") == 1,
                "idle tenant must bank at most one token, released " + scheduler.completedCount("T"));
        for (int i = 0; i < 4; i++) {
            clock.advanceBy(STEP_200MS);
            check(scheduler.awaitIdle(10000), "idle at burst step " + i);
        }
        check(scheduler.completedCount("T") == 5,
                "expected 5 releases by t=5.8s, got " + scheduler.completedCount("T"));
        clock.advanceTo(6 * SECOND - 1);
        check(scheduler.awaitIdle(10000), "idle just before 6s");
        check(scheduler.completedCount("T") == 5,
                "window [5s,6s) must not exceed quota, got " + scheduler.completedCount("T"));
        clock.advanceBy(1);
        clock.advanceBy(STEP_200MS);
        check(scheduler.awaitIdle(10000), "idle at 6.2s");
        check(scheduler.completedCount("T") == 7,
                "all 7 burst tasks must complete, got " + scheduler.completedCount("T"));
        scheduler.shutdown();
    }

    private static void testValidationAndLifecycle() throws Exception {
        Scheduler scheduler = new Scheduler(new ManualClock(), 100);
        expectIllegalArgument(new TestBody() {
            public void run() {
                scheduler.register("x", 0, 1);
            }
        }, "tokensPerSecond=0");
        expectIllegalArgument(new TestBody() {
            public void run() {
                scheduler.register("x", 1, 0);
            }
        }, "weight=0");
        scheduler.register("x", 10, 1);
        expectIllegalArgument(new TestBody() {
            public void run() {
                scheduler.register("x", 10, 1);
            }
        }, "duplicate register");
        expectIllegalArgument(new TestBody() {
            public void run() {
                scheduler.submit("ghost", NOOP);
            }
        }, "unknown tenant");
        scheduler.start();
        scheduler.start(); // idempotent
        scheduler.shutdown();
        try {
            scheduler.submit("x", NOOP);
            throw new AssertionError("submit after shutdown must fail");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    private static void testSystemClockSmoke() throws Exception {
        Scheduler scheduler = new Scheduler(new SystemClock(), 10000);
        scheduler.register("T", 10000, 1);
        scheduler.start();
        for (int i = 0; i < 5; i++) {
            scheduler.submit("T", NOOP);
        }
        check(scheduler.awaitIdle(10000), "system clock scheduler did not drain 5 tasks");
        check(scheduler.completedCount("T") == 5, "expected 5 completions");
        scheduler.shutdown();
    }

    private static void assertShare(String tenant, long actual, long total, int weight, int weightSum) {
        double expected = total * (double) weight / weightSum;
        check(Math.abs(actual - expected) <= 0.15 * expected,
                tenant + " completed " + actual + " of " + total
                        + ", expected " + expected + " within 15%");
    }

    private static void expectIllegalArgument(TestBody body, String what) {
        try {
            body.run();
            throw new AssertionError("expected IllegalArgumentException for " + what);
        } catch (IllegalArgumentException expected) {
            // expected
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError("expected IllegalArgumentException for " + what + ", got " + e);
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
