# MiniJob

独立运行于 .NET 10 的 CPU Job System，采用接近 Unity Jobs 的使用模型。保留 `Schedule(Action, ...)`、`ParallelFor`、`jobs.Complete(handle)`，增加结构体任务、值类型句柄和显式数据安全检查。

```powershell
dotnet run --project MiniJob.csproj -c Release
dotnet run --project MiniJob.csproj -c Release -- --test
```

第二条命令运行无第三方依赖的回归测试；任一失败返回非零退出码。测试包含真实并发、依赖图、异常、并发提交/等待、关闭竞争和缓冲区安全检查，并设有死锁超时保护。

## 基本使用

```csharp
using var jobs = new JobSystem(); // 默认 CPU 数量减一，至少一个 worker
JobHandle a = jobs.Schedule(WorkA);
JobHandle b = jobs.Schedule(WorkB);
JobHandle combined = JobHandle.CombineDependencies(a, b);
JobHandle c = jobs.Schedule(WorkC, combined);
// 这里可以继续做其他工作。
c.Complete();
```

无依赖的任务可以并行；有依赖的任务等全部前置结束才进入就绪队列。等待依赖不占用 worker。只允许依赖已经创建的同一系统句柄，不能后补依赖，因而公开 API 不会构造依赖环。

### 结构体任务与并行循环

```csharp
struct Move : IJobFor, IJobResources
{
    public JobBuffer<float> Positions;
    public JobBuffer<float> Velocities;
    public float DeltaTime;

    public void Execute(int index)
        => Positions[index] += Velocities[index] * DeltaTime;

    public JobAccess[] GetAccesses()
        => new[] { Positions.ReadWrite, Velocities.ReadOnly };
}

// 在 main / 方法体中：
using var positions = new JobBuffer<float>(1000);
using var velocities = new JobBuffer<float>(1000);
using var jobs = new JobSystem(4);
for (int i = 0; i < velocities.Length; i++) velocities[i] = 60;
var job = new Move { Positions = positions, Velocities = velocities, DeltaTime = 1f / 60f };
job.ScheduleParallel(jobs, positions.Length, 64).Complete();
Console.WriteLine(positions[0]);
```

- `IJob.Execute()`：单个任务，`job.Schedule(jobs, dependencies)`。
- `IJobFor.Execute(index)`：`Schedule(jobs, length, dependencies)` 串行遍历；`ScheduleParallel(jobs, length, batchSize, dependencies)` 分批并行。
- `IJobParallelFor`：继承 `IJobFor`，支持 `Schedule(jobs, length, batchSize, dependencies)`。
- `Run(jobs, ...)`：等待前置完成后，在调用线程同步执行，不提交到 worker。
- 调度时复制 job struct；引用字段仍是浅拷贝，不是任意对象的深拷贝。并行 runner 各自持有 struct 副本；不要依赖 `Execute` 修改结构体字段来跨索引汇总结果。
- 并行循环创建至多 `WorkerCount` 个 runner，通过原子游标动态领取 batch，不为每一个 batch 分配句柄。每个索引在成功执行时恰好调用一次，索引之间无顺序保证。
- `length == 0` 仍然保留前置依赖和失败传播语义。

### 句柄、同步和提交

| API | 行为 |
| --- | --- |
| `default(JobHandle)` | 已完成的空依赖 |
| `handle.IsCompleted` / `jobs.IsCompleted(handle)` | 非阻塞查询；不回收缓冲区访问权 |
| `handle.Complete()` / `jobs.Complete(handle)` | 等待并同步整条前置依赖图，释放其缓冲区预约，然后报告异常 |
| `JobHandle.CombineDependencies(...)` / `jobs.CombineDependencies(...)` | 返回所有输入任务的汇合句柄 |
| `JobHandle.CompleteAll(...)` / `jobs.CompleteAll(...)` | 即使部分任务失败，也等待所有输入，再聚合报告失败 |
| `jobs.ScheduleBatchedJobs()` | 提交暂存任务，不等待 |

`new JobSystem(workerCount, deferScheduling: true)` 启用暂存调度。默认立即提交，以兼容旧版本。`Complete`、`Run`、`PumpMainThread` 和 `Dispose` 会自动提交已经暂存的任务。后续新提交的任务仍遵守暂存设置。

`Complete` 的调用线程可以协助执行目标依赖图中的就绪任务，不执行无关任务。多个外部线程可以并发提交和等待。job 内允许提交其他 job，但禁止 `Complete`、`Run`、`PumpMainThread` 和 `Dispose`，避免嵌套阻塞导致线程池死锁。父任务完成不会隐式等待其动态提交的子任务；应显式返回/传递子句柄，在外部组合。

### 受检查的缓冲区

`JobBuffer<T>` 只接受 `unmanaged` 元素，底层仍是托管数组；不暴露原始数组或可绕过检查的 ref/span。

- 声明 `ReadOnly`、`WriteOnly` 或 `ReadWrite`。结构体通过 `IJobResources` 声明，委托通过 `Schedule(action, accesses, dependencies)` 等重载声明。
- 多个只读任务可以并发。只要存在写入，调度时必须通过依赖串联所有冲突任务，或先 `Complete`；缺少依赖立即报错。
- 检查普通线程与 job 的冲突、未声明的访问、访问权限、越界、释放后访问，以及仍被 job 使用时的释放。
- `IsCompleted == true` 仍不能代替 `Complete`。与已预约写入冲突的访问，在同步之前仍被禁止；普通线程可以与只读任务一起读取。
- 并行任务对声明为可写的缓冲区，只能读写当前 `Execute(index)` 对应的元素。只读缓冲区允许任意索引读取。
- 所有检查始终开启，目前通过共享检查锁实现，优先保证可理解的正确性，不是低开销 NativeArray 的性能替代品。
- 普通数组、静态变量、闭包和其他共享对象不受自动保护。普通线程之间的复合操作，例如两个线程同时 `buffer[0]++`，也不是原子事务。

需要跨系统移交缓冲区时，先在原系统 `Complete`；跨系统句柄依赖不被接受。读取任务结果或释放缓冲区之前，需要同步所有使用它的任务。推荐先声明 buffer 的 `using`，再声明 jobs 的 `using`，确保退出作用域时先排空 scheduler，再释放 buffer。

### 主线程交接

这里的“主线程”指构造 `JobSystem` 的线程。

```csharp
JobHandle calculate = jobs.Schedule(Calculate);
JobHandle apply = jobs.ScheduleMainThread(ApplyToScene, calculate);
// 每帧由创建线程调用，执行已就绪的主线程任务：
jobs.PumpMainThread(maxJobs: 8);
// 或在必须取得结果时：
apply.Complete();
```

如果回调读取 `JobBuffer`，使用带访问声明的重载，例如 `ScheduleMainThread(() => Use(buffer[0]), new[] { buffer.ReadOnly }, calculate)`。

`PumpMainThread` 只执行就绪的主线程任务，不等待后台任务，也不会自动同步并释放句柄的缓冲区预约。仍需 `Complete`。外部线程可以等待主线程任务，但创建线程必须继续 pump/complete；不要让创建线程阻塞等待一个正在等主线程任务的外部线程。

### 异常和退出

异常被捕获，worker 不因此退出。后继任务等所有前置结束后跳过执行，并继承失败。多条失败分支汇合时聚合异常；单个失败保留原始异常及堆栈。并行循环失败不会回滚已经完成的写入，其他正在运行的 runner 可能继续执行。

`Dispose` 必须由创建线程调用，拒绝新任务，提交并排空已有任务（包括主线程队列），加入 worker，并释放剩余缓冲区预约；可以重复调用。关闭开始后，正在执行的 job 再提交子任务也会收到 `ObjectDisposedException`。`Dispose` 不抛出未观察的 job 失败；需要错误结果时先调用对应句柄的 `Complete`。

系统不会强制中断用户代码。永不返回的 job、用户自己的互斥死锁或阻塞 I/O 会使等待/关闭无法完成。传给 `Action` 的回调必须是同步代码，不要传 `async void`。

## 与 Unity 的边界

这是 Unity 风格的独立托管调度器，不是 Unity API 的二进制或完整行为兼容实现。

- 已有：结构体任务、值类型句柄、依赖图、并行循环、动态批次领取、等待协助执行、批量提交、显式容器安全检查；另提供主线程队列和托管异常传播。
- 暂未实现：Unity 原生调度器/内存分配器、NativeArray/NativeList 全家桶、Burst/SIMD 编译、JobsUtility 自定义 producer、Transform/Physics/Entities 专用 jobs、Profiler 集成、容器依赖释放。
- 调度队列仍为共享队列与锁，没有伪称实现每 worker 队列的 work stealing。没有优先级、取消、外部事件 fence 或父子自动完成关联；这些是可选扩展，不是 Unity 核心使用模型的必要条件。
- 实例由调用者明确传入，不使用 Unity 的引擎全局调度器；接受多线程提交，使用显式访问声明而非字段属性反射；`Run` 会建立轻量任务节点以复用安全检查。
- 必须对完成句柄调用 `Complete`（或最终 `Dispose`）释放内部记录；只轮询 `IsCompleted` 会保留记录和安全预约。

实现参考的是官方语义文档，没有复制第三方引擎源代码：

- [Unity JobHandle](https://docs.unity3d.com/6000.0/Documentation/ScriptReference/Unity.Jobs.JobHandle.html)
- [Unity IJobFor](https://docs.unity3d.com/6000.0/Documentation/ScriptReference/Unity.Jobs.IJobFor.html)
- [Unity 创建和运行任务](https://docs.unity3d.com/6000.0/Documentation/Manual/job-system-creating-jobs.html)
- [Unity NativeContainer](https://docs.unity3d.com/6000.0/Documentation/Manual/job-system-native-container.html)

`job_system.py` 保留为原始单线程队列示例，不属于此 C# 调度器。
