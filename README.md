# com.gsb.quota — 多租户配额 + 加权公平调度器

纯 JDK 8 标准库实现，源码在 `src/` 下，无 Maven/Gradle/JUnit 及任何第三方依赖。

## 运行测试

```sh
./run-tests.sh
```

脚本用 `javac --release 8`（老 JDK 回退 `-source 1.8 -target 1.8`）编译
`src/` 下全部源码并运行 `com.gsb.quota.TestMain`。所有耗时用例都用
`ManualClock` 虚拟时间推进，整套测试实际耗时在秒级以内。

## 对外 API（`com.gsb.quota.Scheduler`）

- `void register(String tenantId, long tokensPerSecond, int weight)`
- `String submit(String tenantId, Callable task)` — 返回任务 id；超配额的提交进入等待队列，绝不丢弃
- `void start()` / `void shutdown()`

构造器注入 `Clock`：`new Scheduler(clock, maxTasksPerSecond)`。

## 设计

- **加权公平**：调度器自身是稀缺资源（全局放行速率上限）。单调度线程按
  WFQ 选择下一个放行的租户：在"队列非空且令牌可用"的租户中取虚拟完成时间
  `virtualTime + 1/weight` 最小者，每放行一个任务该租户虚拟时间前进
  `1/weight` 虚拟秒。长期完成量比例收敛到权重比；可证明任何有积压的租户，
  其虚拟时间落后最多 `1/weight ≤ 1` 虚拟秒（实现中持续跟踪并通过
  `maxVirtualLag` 暴露，测试直接断言）。
- **配额**：每租户一个容量为 1 的令牌桶，按 `tokensPerSecond` 速率补充，
  令牌间隔取 `ceil(1e9 / tokensPerSecond)` 纳秒。放行间隔因此至少为一个令牌
  间隔，任意 1 秒滑窗内放行数不超过配额；空闲期间最多攒 1 个令牌，不会在
  窗口边界上多放一个。
- **时间**：实现里所有时间读取都经过可注入的 `Clock`
  （`nanoTime` + `scheduleWakeup`），没有 `System.currentTimeMillis`，
  也没有 `Thread.sleep`（等待用 `Object.wait` / 时钟回调唤醒）。
  生产环境用 `SystemClock`，测试用 `ManualClock`。
