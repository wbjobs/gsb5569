package com.gsb.quota;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Token bucket boundary: with a quota of exactly 5 tokens/second, exactly 5
 * tasks are released in the first window and not one more. The 6th task must
 * wait in the queue until the next window opens at exactly t=1000 ms.
 */
final class TokenBucketBoundaryTest implements TestCase {

    private static final long QUOTA = 5L;

    public String name() {
        return "token-bucket-boundary";
    }

    public void run() {
        final ManualClock clock = new ManualClock();
        final Scheduler scheduler = new Scheduler(clock);
        scheduler.register("t", QUOTA, 1);

        final AtomicInteger completed = new AtomicInteger();
        Callable<Object> task = new Callable<Object>() {
            public Object call() {
                completed.incrementAndGet();
                return null;
            }
        };

        // Exactly at the quota boundary: all 5 must be released.
        for (int i = 0; i < (int) QUOTA; i++) {
            scheduler.submit("t", task);
        }
        scheduler.start();
        scheduler.awaitIdle();
        Check.equals(QUOTA, completed.get(), "exactly the quota must be released");

        // One more: must be queued, not released, not dropped.
        scheduler.submit("t", task);
        scheduler.awaitIdle();
        Check.equals(QUOTA, completed.get(), "the bucket must not release one token above the quota");

        // Just before the window boundary: still nothing.
        clock.advance(999L);
        scheduler.awaitIdle();
        Check.equals(QUOTA, completed.get(), "no release before the window rolls over");

        // Exactly at the boundary: the queued task is released.
        clock.advance(1L);
        scheduler.awaitIdle();
        Check.equals(QUOTA + 1L, completed.get(), "queued task must be released in the new window");

        // The new window holds exactly QUOTA tokens again: one was consumed
        // above, so of 5 more submissions exactly QUOTA-1 are released now.
        for (int i = 0; i < (int) QUOTA; i++) {
            scheduler.submit("t", task);
        }
        scheduler.awaitIdle();
        Check.equals(2L * QUOTA, completed.get(), "second window must also stop exactly at its quota");

        clock.advance(1000L);
        scheduler.awaitIdle();
        Check.equals(2L * QUOTA + 1L, completed.get(), "leftover task must complete in the third window");

        scheduler.shutdown();
    }
}
