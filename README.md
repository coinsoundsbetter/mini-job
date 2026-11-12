# MiniJob

一个使用 C# / .NET 10 实现的轻量 Job System，用于学习任务调度、依赖管理、工作窃取和线程同步。

当前是**全局锁版本**：固定数量的 worker 共享一个调度器，每个 worker 拥有自己的任务队列；队列操作和依赖状态由同一把锁保护，实际任务计算在锁外并行执行。项目以简单、可理解为目标，作为后续性能优化的基线。

## 当前支持

- `IJob`：执行一次的任务。
- `IJobFor`：按索引执行，可拆成批次并行处理。
- 任务依赖与 `CombineDependencies` 多任务汇合。
- 工作窃取：优先取自己队列的队尾，空闲时取其他队列的队首。
- 句柄状态查询、阻塞等待和异常汇总。
- 依赖失败后跳过后续任务，传播时去重同一次失败。
- 关闭时拒绝新任务，等待已提交任务结束，再退出工作线程。

## 运行

需要安装 **.NET 10 SDK**，无需额外第三方包。在项目目录执行：

```shell
# 平方计算示例，输出：1, 4, 9, 16
dotnet run -c Release

# 6 项回归测试，包含 30 秒超时保护
dotnet run -c Release -- --test

# 串行与 Job 并行计算对比
dotnet run -c Release -- --benchmark

# 指定 4 个 worker 进行性能对比
dotnet run -c Release -- --benchmark 4
```

## 使用示例

以下示例可替换 `Program.cs`，演示初始化数组，再依赖初始化任务进行并行平方计算：

```csharp
using System;
using MiniJob;

public static class Program
{
    public static void Main()
    {
        using var scheduler = new JobScheduler(new JobSchedulerOptions
        {
            WorkerCount = 4
        });

        var values = new int[4];

        var prepare = scheduler.Schedule(new PrepareJob { Values = values });
        var square = scheduler.ScheduleParallel(
            new SquareJob { Values = values },
            length: values.Length,
            batchSize: 2,
            dependsOn: prepare);

        // 创建线程等待；成功返回后再使用计算结果。
        square.Complete();
        Console.WriteLine(string.Join(", ", values)); // 1, 4, 9, 16
    }

    private struct PrepareJob : IJob
    {
        public int[] Values;

        public void Execute()
        {
            for (int i = 0; i < Values.Length; i++)
                Values[i] = i + 1;
        }
    }

    private struct SquareJob : IJobFor
    {
        public int[] Values;

        public void Execute(int index)
        {
            Values[index] *= Values[index];
        }
    }
}
```

多个前置任务可以先合并句柄，再交给后续任务：

```csharp
// a、b 是同一调度器提交的任务句柄，nextJob 是后续 IJob。
var dependencies = JobHandle.CombineDependencies(a, b);
var next = scheduler.Schedule(nextJob, dependencies);
next.Complete();
```

合并本身不阻塞调用线程，也不占用 worker 等待。所有前置任务结束后，调度器才决定执行或跳过后续任务。

## 主要 API

| API | 作用 |
|---|---|
| `Schedule(job, dependsOn)` | 提交一次执行的任务 |
| `ScheduleParallel(job, length, batchSize, dependsOn)` | 按索引拆成批次提交 |
| `Run(job)` / `RunFor(job, length)` | 在调用线程同步执行，不经过工作队列 |
| `JobHandle.CombineDependencies(...)` | 合并多个前置依赖 |
| `handle.IsCompleted` / `handle.Status` | 查询是否结束及具体状态 |
| `handle.Complete()` | 等待结束，失败时抛出 `AggregateException` |
| `scheduler.Dispose()` | 等待已有任务完成并关闭 worker |

默认 worker 数量为 `Math.Max(1, Environment.ProcessorCount - 1)`，也可通过 `JobSchedulerOptions` 显式指定正整数。

## 执行过程

```text
提交任务 → 创建 JobNode → 等待前置依赖
    → 依赖成功：拆分 WorkItem 批次并入队
    → worker 领取或窃取批次，在锁外执行
    → 所有批次结束：更新终态，通知后续任务与等待线程
```

`JobNode` 表示一个逻辑任务，`WorkItem` 表示其中一个索引范围，`JobHandle` 是调用者使用的句柄。一个任务即使拆成多个批次，也只有一个最终完成状态。

没有任务时 worker 使用 `Monitor.Wait` 释放锁并休眠；入队、任务完成和关闭时，通过 `Monitor.PulseAll` 通知等待线程重新检查条件。

## 使用约定与边界

- 按全局共享一个调度器使用；只有创建它的线程可以提交任务和创建非空依赖汇合节点，不支持 worker 内提交子任务。
- worker 不能通过 `Complete()` 等待未完成任务，应在提交时声明依赖；也不能关闭自己的调度器。
- job 必须是结构体。结构体复制不会深拷贝数组等引用字段，共享数据的并发访问由调用者负责。按索引执行时，每次调用使用独立的结构体副本。
- `IsCompleted` 只表示进入终态，包括 `Succeeded`、`Faulted` 和 `Skipped`；需要通过 `Complete()` 检查失败。默认句柄表示成功完成。
- 批次执行顺序不保证。某个索引抛异常会停止当前批次剩余索引，其他批次继续，整个任务等待所有批次收尾后才结束。
- 依赖失败会跳过后续任务；零长度任务仍需等待依赖，依赖成功后直接完成。
- `Dispose()` 可重复调用，但会阻塞等待，不强制取消任务，也不重新抛出任务异常。用户任务一直不结束，关闭也会一直等待。
- 当前不提供取消、优先级、队列容量限制、无锁队列、原生内存安全检查或 Unity/Burst 集成。

## 性能说明

全局锁让提交、领取、依赖更新和收尾操作串行；业务计算仍可并行。批次入队会分配链表节点，因此当前版本也不是零分配实现。

微小任务可能比普通循环更慢。适当增大 `batchSize` 可以减少分配和锁竞争，但批次过大会降低负载均衡能力，应按实际工作量测量。

内置 benchmark 对比微小、轻量和密集计算，包含提交与等待成本，不包含调度器创建；预热后测量 7 轮并逐元素校验结果。结果受硬件与系统负载影响，不保证固定加速比。

## 文件导航

| 文件 | 内容 |
|---|---|
| `JobScheduler.cs` | 调度、工作窃取、依赖传播与关闭 |
| `JobNode.cs` / `JobHandle.cs` | 内部任务状态、失败记录与外部句柄 |
| `IJob.cs` / `IJobFor.cs` | 任务接口 |
| `JobSchedulerOptions.cs` / `JobStatus.cs` | 配置与任务状态 |
| `Program.cs` | 示例及测试、benchmark 入口 |
| `JobSchedulerTests.cs` | 等待防护、异常去重、批次收尾和关闭的回归验证 |
| `JobBenchmarks.cs` | 串行与并行性能对比 |

更详细的学习记录见 [Job System 学习笔记](JobSystem%20note.md)。
