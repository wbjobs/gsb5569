package com.gsb.quota;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic API behaviour: validation, unique task ids, a task actually runs,
 * shutdown is idempotent.
 */
final class SmokeTest implements TestCase {

    public String name() {
        return "smoke";
    }

    public void run() {
        ManualClock clock = new ManualClock();
        final Scheduler scheduler = new Scheduler(clock);
        scheduler.register("a", 10L, 1);

        expectThrows(IllegalArgumentException.class, new Runnable() {
            public void run() {
                scheduler.submit("no-such-tenant", noop());
            }
        }, "submit to unknown tenant must fail");

        expectThrows(IllegalStateException.class, new Runnable() {
            public void run() {
                scheduler.register("a", 10L, 1);
            }
        }, "duplicate register must fail");

        expectThrows(IllegalArgumentException.class, new Runnable() {
            public void run() {
                scheduler.register("b", 0L, 1);
            }
        }, "non-positive quota must fail");

        expectThrows(IllegalArgumentException.class, new Runnable() {
            public void run() {
                scheduler.register("c", 10L, 0);
            }
        }, "non-positive weight must fail");

        final AtomicInteger ran = new AtomicInteger();
        Set<String> ids = new HashSet<String>();
        for (int i = 0; i < 3; i++) {
            String id = scheduler.submit("a", new Callable<Object>() {
                public Object call() {
                    ran.incrementAndGet();
                    return null;
                }
            });
            Check.isTrue(id != null && !ids.contains(id), "task ids must be unique");
            ids.add(id);
        }

        scheduler.start();
        scheduler.awaitIdle();
        Check.equals(3L, ran.get(), "all submitted tasks must run");

        scheduler.shutdown();
        scheduler.shutdown(); // idempotent
    }

    private static Callable<Object> noop() {
        return new Callable<Object>() {
            public Object call() {
                return null;
            }
        };
    }

    private static void expectThrows(Class<? extends Throwable> type, Runnable action, String message) {
        try {
            action.run();
        } catch (Throwable t) {
            if (type.isInstance(t)) {
                return;
            }
            throw new AssertionError(message + " (threw " + t + " instead)");
        }
        throw new AssertionError(message + " (nothing was thrown)");
    }
}
