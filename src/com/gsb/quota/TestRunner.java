package com.gsb.quota;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Minimal test harness (no JUnit). Runs every TestCase, prints PASS/FAIL and
 * exits non-zero if anything failed. A watchdog kills the JVM if the whole
 * suite stalls (e.g. a scheduler deadlock).
 */
public final class TestRunner {

    private static final long WATCHDOG_SECONDS = 120L;

    private TestRunner() {
    }

    public static void main(String[] args) {
        final CountDownLatch done = new CountDownLatch(1);
        Thread watchdog = new Thread(new Runnable() {
            public void run() {
                try {
                    if (!done.await(WATCHDOG_SECONDS, TimeUnit.SECONDS)) {
                        System.out.println("FAIL: global watchdog timeout, possible deadlock");
                        Runtime.getRuntime().halt(2);
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "test-watchdog");
        watchdog.setDaemon(true);
        watchdog.start();

        TestCase[] tests = new TestCase[] {
            new SmokeTest(),
            new FairnessTest(),
            new QuotaWindowTest(),
            new TokenBucketBoundaryTest()
        };

        int failed = 0;
        for (int i = 0; i < tests.length; i++) {
            TestCase test = tests[i];
            long started = System.nanoTime();
            try {
                test.run();
                long tookMs = (System.nanoTime() - started) / 1000000L;
                System.out.println("PASS " + test.name() + " (" + tookMs + " ms)");
            } catch (Throwable t) {
                failed++;
                System.out.println("FAIL " + test.name() + ": " + t);
                t.printStackTrace(System.out);
            }
        }

        done.countDown();
        System.out.println(failed == 0
                ? "ALL " + tests.length + " TESTS PASSED"
                : failed + " of " + tests.length + " tests FAILED");
        System.exit(failed == 0 ? 0 : 1);
    }
}
