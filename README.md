# 多租户配额 + 加权公平调度器 (com.gsb.quota)

JDK 8，仅标准库，无 Maven/Gradle/JUnit。

## 设计

- **配额**：每个租户一个令牌桶，每秒窗口开始时补满至 `tokensPerSecond`。
  任一按秒对齐的窗口内放行量 ≤ 配额；超出的提交进入租户 FIFO 队列等待，绝不丢弃。
- **公平**：加权公平队列（WFQ）。每个租户维护虚拟完成时间，每次放行
  增加 `SCALE / weight`；调度器总是选择虚拟完成时间最小且持有令牌的
  租户。完成量长期收敛到权重比，任何租户的饥饿有界。
- **并发**：提交路径只有每租户一把锁（`ConcurrentHashMap` + 租户级锁），
  无全局锁；单个调度线程负责选择与执行。
- **时间**：所有读时与等待都经过可注入的 `com.gsb.quota.Clock`
  （`SystemClock` 基于 `System.nanoTime()`，测试用 `ManualClock` 虚拟推进）。
  实现中不含 `System.currentTimeMillis` 与 `Thread.sleep`。

## API

```java
Scheduler s = new Scheduler();            // 或 new Scheduler(clock)
s.register("tenant", 1000L, 2);           // tenantId, tokensPerSecond, weight
String id = s.submit("tenant", callable); // 返回任务 id，永不丢弃
s.start();
s.shutdown();
```

## 运行测试

```bash
./run-tests.sh
```

编译 `src/` 下全部源码（JDK 9+ 用 `--release 8`，JDK 8 用 `-source/-target 8`），
然后运行 `com.gsb.quota.TestRunner`：全部测试用虚拟时间推进，秒级跑完。

用例：
- `FairnessTest`：权重 1:2:3，30 秒虚拟时间后完成量比例偏差 ≤ 15%，
  且任何租户连续完成间隔 ≤ 1000 ms 虚拟时间（无饥饿）。
- `QuotaWindowTest`：按秒窗口统计放行量不超过配额，超额部分排队不丢弃。
- `TokenBucketBoundaryTest`：恰好等于配额时不多放一个，窗口边界精确滚动。
- `SmokeTest`：参数校验、任务 id 唯一、shutdown 幂等。
