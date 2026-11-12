# Job System note

## 1. 当前版本的定位

这是一个单调度器、固定 worker 线程、全局锁保护调度状态的 Job System。

- 创建线程提交任务，worker 执行任务。
- 每个 worker 有自己的双端队列，空闲时可以窃取其他队列的工作。
- 队列和依赖状态共用 `gate` 锁，实际任务计算在锁外执行。
- 当前优先保证简单和正确，不是无锁实现。

## 2. 三个核心对象

| 对象 | 作用 |
|---|---|
| `JobNode` | 一个逻辑任务，保存状态、执行函数、依赖和异常 |
| `WorkItem` | worker 领取的一个批次，包含节点和索引范围 `[Begin, End)` |
| `JobHandle` | 调用者持有的句柄，用于建立依赖、查询状态和等待完成 |

一个 job 可以拆成多个批次，但只有一个逻辑完成状态。所有批次结束，整个 job 才结束。

## 3. 任务如何运转

```text
Schedule / ScheduleParallel
    → Submit：创建节点，登记依赖
    → 依赖全部结束
    → Activate：依赖失败则跳过，长度为 0 则直接完成
    → Enqueue：拆分批次，加入队列，通知 worker
    → TryTake：领取自己的任务，或窃取其他队列的任务
    → Execute：锁外执行批次，锁内记录结果
    → 最后一个批次结束，调用 Finish
    → 设置终态，通知后续任务和等待线程
```

`Schedule` 提交一次执行；`ScheduleParallel` 按批次处理索引。`Run` 和 `RunFor` 则直接在调用线程执行，不经过队列。

并行任务的结构体按索引复制，但数组等引用字段仍共享同一个对象。调用者需要避免多个任务无同步地修改同一份数据。

## 4. 依赖关系

```csharp
var a = scheduler.Schedule(jobA);
var b = scheduler.Schedule(jobB, a);
b.Complete();
```

表示 B 依赖 A，A 成功后才能执行 B。

- `B.Dependencies`：B 尚未等到的前置任务数量。
- `A.Dependents`：依赖 A 的后续任务，包含 B。
- 等待依赖的任务不进入工作队列，不占用 worker。
- 前置任务失败，后续任务进入 `Skipped`。
- `CombineDependencies` 创建一个无计算的汇合节点，等待多个输入结束。

使用“前置任务”和“后续任务”描述关系，比“父节点”和“子节点”更明确。

## 5. worker 与工作窃取

worker 持续执行：领取 → 执行 → 再领取。没有任务时休眠，停止后退出。

- 从自己的队尾取任务。
- 自己没有任务，从其他队列的队首窃取。
- `(index + offset) % queues.Length` 实现环形遍历，跳过自己。
- 批次之间不保证执行顺序；同一批次内按索引顺序执行。

## 6. lock、Wait 和 PulseAll

线程同步是协调访问的手段，线程安全是保证行为正确的目标。`lock` 提供互斥：同一时刻只有一个线程能持有同一个 `gate` 锁。

```csharp
lock (gate)                 // 取得锁
{
    while (!condition)
        Monitor.Wait(gate); // 等待期间释放锁，返回前重新取得锁

    // 此时仍持有锁
}                          // 释放锁；return 或抛异常也会释放
```

| 操作 | 含义 |
|---|---|
| `lock (gate)` | 取得锁后进入，离开时释放 |
| `Monitor.Wait(gate)` | 释放锁并暂停；收到通知后，重新取得锁才返回 |
| `Monitor.PulseAll(gate)` | 通知所有在此对象上等待的线程，自己不释放锁 |
| `worker.Join()` | 阻塞调用线程，直到目标 worker 结束 |

关键理解：代码位置仍在 `lock` 内，不代表等待期间一直持有锁。被通知也不代表已经取得锁。

`Wait` 和 `PulseAll` 都要求调用者持有对应的锁。唤醒只表示条件可能改变，必须用 `while` 重新检查；修改变量不会自动唤醒线程，通知也不会被保存供未来等待使用。

## 7. Execute 和 Complete 的区别

`Execute(item)` 执行一个批次，捕获异常，并减少 `node.Remaining`。最后一个批次结束时调用 `Finish`，确定整个 job 成功或失败。

某个索引抛异常会终止当前批次剩余索引，其他批次继续执行。失败记录在依赖汇合时去重，独立发生的失败仍然保留。

`Complete(node)` 不执行任务，也不设置完成状态，只等待节点进入终态，然后检查异常。

- 外部线程可以阻塞等待。
- worker 不能等待尚未完成的任务，应提前声明依赖。
- 否则可能出现 A 占着唯一 worker 等 B，而 B 又等 worker 空闲的死锁。
- `IsCompleted` 表示任务已结束，不代表成功；`Complete` 会用 `AggregateException` 报告失败。

## 8. Dispose 如何关闭

```text
closing = true：拒绝新提交
    → 等待 unfinishedJobCount == 0
    → stopped = true：允许 worker 退出
    → PulseAll：唤醒休眠 worker，让它们检查停止标记
    → 释放 gate 锁
    → Join：确认所有 worker 已退出
```

- `unfinishedJobCount` 统计未结束的逻辑 job，不是批次数。
- `closing` 表示正在关闭，已有任务继续执行。
- `stopped` 表示已有任务全部结束，worker 可以退出。
- 没有任务不等于线程结束：worker 可能仍在 `Wait`，所以需要唤醒它退出。
- `Join` 必须放在锁外，避免主线程持锁等退出、worker 等锁才能退出。
- worker 不能关闭自己的调度器，否则会等待自身任务结束。
- `Dispose` 不取消或强制终止任务，永不返回的任务也会阻止关闭。

## 9. 当前版本的性能边界

全局锁会让提交、领取和收尾操作串行。批次拆分、队列节点分配和依赖传播也在锁内进行。

worker 越多、批次越小、计算越轻，调度开销越容易突出。适当增大 `batchSize` 可以减少同步次数，但过大会降低负载均衡能力。

当前只保留调度需要的 `unfinishedJobCount` 和队列游标 `nextQueue`，移除了纯统计字段。

后续可研究分队列锁、原子操作和无锁工作窃取队列，但不能直接删除现有锁。先测量，再决定是否增加实现复杂度。

## 10. 代码阅读入口与验证

- `JobScheduler.cs`：提交、入队、执行、完成传播和关闭。
- `JobNode.cs` / `JobHandle.cs`：内部状态和外部句柄。
- `JobSchedulerTests.cs`：等待防护、异常传播和关闭等回归测试。
- `JobBenchmarks.cs`：串行与并行计算对比。

```shell
dotnet run -c Release -- --test
dotnet run -c Release -- --benchmark 4
```

当前版本已通过 Release 构建、6 项回归测试和基准输出校验。它可以作为带锁版本的学习与后续优化基线。
