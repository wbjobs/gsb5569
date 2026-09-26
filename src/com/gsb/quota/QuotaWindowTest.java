package com.gsb.quota;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Quota enforcement: a tenant with a quota of 3 tokens/second submits 10
 * tasks. Releases per one-second window must never exceed 3, and the tasks
 * that exceed the quota must wait in the queue instead of being dropped.
 */
final class QuotaWindowTest implements TestCase {

    private static final long QUOTA = 3L;
    private static final int TASKS = 10;

    public String name() {
        return "quota-window";
    }

    public void run() {
        final ManualClock clock = new ManualClock();
        final Scheduler scheduler = new Scheduler(clock);
        scheduler.register("t", QUOTA, 1);

        final List<Long> completedAt = Collections.synchronizedList(new ArrayList<Long>());
        Callable<Object> recorder = new Callable<Object>() {
            public Object call() {
                completedAt.add(Long.valueOf(clock.nowMillis()));
                return null;
            }
        };
        for (int i = 0; i < TASKS; i++) {
            scheduler.submit("t", recorder);
        }

        scheduler.start();

        // Window [0,1000): exactly QUOTA releases, the rest waits in the queue.
        scheduler.awaitIdle();
        Check.equals(QUOTA, completedAt.size(), "window 0 must release exactly the quota");

        clock.advance(1000L);
        scheduler.awaitIdle();
        Check.equals(2L * QUOTA, completedAt.size(), "window 1 must release exactly the quota");

        clock.advance(1000L);
        scheduler.awaitIdle();
        Check.equals(3L * QUOTA, completedAt.size(), "window 2 must release exactly the quota");

        clock.advance(1000L);
        scheduler.awaitIdle();
        Check.equals(TASKS, completedAt.size(),
                "all tasks must eventually complete; none may be dropped");

        // Per-window accounting: no aligned one-second window may exceed QUOTA.
        long[] perWindow = new long[TASKS];
        int maxWindow = 0;
        for (int i = 0; i < completedAt.size(); i++) {
            int window = (int) (completedAt.get(i).longValue() / 1000L);
            perWindow[window]++;
            maxWindow = Math.max(maxWindow, window);
        }
        for (int w = 0; w <= maxWindow; w++) {
            Check.isTrue(perWindow[w] <= QUOTA,
                    "window " + w + " released " + perWindow[w] + " tasks, quota is " + QUOTA);
        }
        long sum = 0L;
        for (int w = 0; w <= maxWindow; w++) {
            sum += perWindow[w];
        }
        Check.equals(TASKS, sum, "windowed counts must add up to all submissions");

        scheduler.shutdown();
    }
}
