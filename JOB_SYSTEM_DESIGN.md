# MiniJob

A small .NET 10, Unity-inspired in-process CPU job scheduler. Intended usage is one shared scheduler; coordination between multiple schedulers is out of scope.

## Run

`dotnet run` runs the square-calculation example. `dotnet run -- --test` runs six regression tests with a 30-second watchdog. No external packages are required.

## API

- Implement `IJob.Execute()` or `IJobFor.Execute(int index)` on a struct.
- Create `JobScheduler`, optionally setting `JobSchedulerOptions.WorkerCount`.
- `Schedule` runs one job after its dependency succeeds.
- `ScheduleParallel` splits indices into batches; batch size must be positive.
- `Run` and `RunFor` execute synchronously on the caller.
- `JobHandle.CombineDependencies` creates a nonblocking join.
- `Complete` waits and throws `AggregateException` for failed or skipped work.
- `Dispose` drains accepted work and joins workers.

```csharp
using var scheduler = new JobScheduler(new() { WorkerCount = 4 });
var a = scheduler.Schedule(new PrepareJob());
var b = scheduler.ScheduleParallel(new UpdateJob(), count, 64, a);
b.Complete();
```

## Contracts

Submission copies the job struct, but reference fields still point to shared objects. Results must be written to explicit shared buffers. The caller is responsible for data races and buffer lifetimes. Per-index jobs receive value copies: mutable job fields must not communicate between indices. This implementation has no Unity NativeContainer safety checks or Burst compilation.

Default handles represent successful completion. Handles are copyable and remain queryable after scheduler disposal. They retain managed completion state; they do not need disposal. IsCompleted means terminal, not necessarily successful. Use Complete before consuming results. Handles from different schedulers cannot be combined or used as dependencies.

Jobs are ready only after all dependencies terminate successfully. Failed dependencies cause Skipped status. Joins wait for all inputs, including when some fail. A zero-length operation still honors its dependency. Parallel failure stops the failing batch at the throwing index; other batches continue, and the handle does not terminate until every batch settles. Subsequent Complete calls report the recorded failure again. Failure records are deduplicated across dependency paths; distinct execution failures remain distinct even if they throw the same exception object.

Only the thread that created the scheduler may submit jobs or combine dependencies. External Complete callers block. Workers may observe completed handles, but Complete on an unfinished handle throws InvalidOperationException; express execution order through dependencies instead. This prevents scheduler-managed waits from exhausting workers or introducing reentrant deadlocks. Worker waits across schedulers are rejected. Blocking inside user code (locks, events, I/O) remains the caller's responsibility.

Dispose rejects all new submissions. It waits for all accepted work, then joins threads. It is idempotent and cannot be invoked by one of its own workers. Disposal drains failures without rethrowing them; retain and Complete handles to observe failures. A user job that never returns can prevent disposal indefinitely.

## Implementation and limits

Fixed background workers own double-ended linked queues. Owners pop from the back; idle workers steal from other queues' fronts. One monitor protects queues, graph updates, terminal states and admission. Workers sleep on that same monitor, so enqueue/completion/shutdown notifications cannot be lost. This is a correctness-oriented work-stealing implementation, not a lock-free or allocation-free scheduler.

Dependency completion propagation is iterative. A completed node releases its callable and dependent links. A parallel schedule is one logical job with multiple work items.

Cancellation, priorities, native memory, bounded queues, affinity, automatic cycle detection and tracing are not implemented.

## Performance comparison

Run `dotnet run -c Release -- --benchmark` to compare a direct loop against ScheduleParallel + Complete. Optionally specify worker count: `dotnet run -c Release -- --benchmark 4`. Run with `--test` to execute regression checks.

JobBenchmarks.cs covers tiny, light and compute-heavy integer workloads. Both paths call the same kernel with identical inputs and separate preallocated output buffers. Every measured round checks exact output equality outside the timer. Each scenario warms up both paths for at least 300 ms, uses the same repetition count for both paths, alternates execution order over seven rounds, and reports median/min/max per-operation wall time and sequential/job speedup. The median is calculated over round averages, not individual job latencies.

Job timings include submission, queue allocation, worker execution and Complete. Scheduler construction, input/output allocation, warmup and validation are excluded. This measures reuse of an existing worker pool. Tiny/light jobs can be slower because dispatch, synchronization and queue costs exceed the computation saved. These are illustrative local measurements, not a guarantee of speedup on other machines or application workloads; background load, JIT tiering and CPU power states can affect results. No timing threshold is asserted.



