using System;
using System.Threading;
using MiniJob;

internal static class JobSchedulerTests
{
    private struct ActionJob : IJob
    {
        public Action Body;
        public void Execute() => Body();
    }

    private struct IndexJob : IJobFor
    {
        public Action<int> Body;
        public void Execute(int index) => Body(index);
    }

    internal static void Run()
    {
        // 独立 watchdog 覆盖 Complete 和 Dispose，避免回归死锁挂住测试进程。
        using var deadline = new Timer(_ => Environment.FailFast("Scheduler tests timed out."),
            null, TimeSpan.FromSeconds(30), Timeout.InfiniteTimeSpan);
        WorkerWaitFailsFast();
        CrossWorkerWaitFailsFast();
        DiamondFailuresAreUnique();
        DistinctFailuresArePreserved();
        ParallelFailureDrainsAllBatches();
        DependenciesAndDisposal();
        Console.WriteLine("All 6 scheduler regression tests passed.");
    }

    private static void Check(bool condition, string message)
    {
        if (!condition) throw new Exception(message);
    }

    private static AggregateException Failure(JobHandle handle)
    {
        try { handle.Complete(); }
        catch (AggregateException error) { return error; }
        throw new Exception("Expected job failure.");
    }

    private static void WorkerWaitFailsFast()
    {
        using var scheduler = new JobScheduler(new() { WorkerCount = 1 });
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        JobHandle b = default;
        var a = scheduler.Schedule(new ActionJob { Body = () =>
        {
            entered.Set();
            release.Wait();
            b.Complete();
        } });
        Check(entered.Wait(5000), "Worker failed to start.");
        b = scheduler.Schedule(new ActionJob { Body = () => { } });
        var c = scheduler.Schedule(new ActionJob { Body = () => a.Complete() });
        release.Set();
        Check(Failure(a).InnerException is InvalidOperationException, "Worker wait must fail fast.");
        Failure(c);
        b.Complete();
    }

    private static void CrossWorkerWaitFailsFast()
    {
        using var scheduler = new JobScheduler(new() { WorkerCount = 2 });
        using var entered = new CountdownEvent(2);
        using var release = new ManualResetEventSlim();
        JobHandle a = default, b = default;
        a = scheduler.Schedule(new ActionJob { Body = () => { entered.Signal(); release.Wait(); b.Complete(); } });
        b = scheduler.Schedule(new ActionJob { Body = () => { entered.Signal(); release.Wait(); a.Complete(); } });
        Check(entered.Wait(5000), "Both workers must start.");
        release.Set();
        Failure(a);
        Failure(b);
    }

    private static void DiamondFailuresAreUnique()
    {
        using var scheduler = new JobScheduler(new() { WorkerCount = 2 });
        using var release = new ManualResetEventSlim();
        var root = new Exception("root");
        var handle = scheduler.Schedule(new ActionJob { Body = () => { release.Wait(); throw root; } });
        for (int i = 0; i < 24; i++)
        {
            var left = scheduler.Schedule(new ActionJob { Body = () => throw new Exception("Must skip") }, handle);
            var right = scheduler.Schedule(new ActionJob { Body = () => throw new Exception("Must skip") }, handle);
            handle = JobHandle.CombineDependencies(left, right);
        }
        release.Set();
        var error = Failure(handle);
        Check(error.InnerExceptions.Count == 1 && ReferenceEquals(error.InnerException, root), "Diamond duplicated a failure.");
        Check(handle.Status == JobStatus.Skipped, "Failed join must be skipped.");
        // 也覆盖提交时依赖已经失败的传播分支。
        var joined = JobHandle.CombineDependencies(handle, scheduler.Schedule(new ActionJob(), handle));
        Check(Failure(joined).InnerExceptions.Count == 1, "Completed dependency duplicated a failure.");
        Check(Failure(joined).InnerExceptions.Count == 1, "Repeated Complete changed failures.");
    }

    private static void DistinctFailuresArePreserved()
    {
        using var scheduler = new JobScheduler(new() { WorkerCount = 2 });
        var sameException = new Exception("Thrown by two jobs");
        var a = scheduler.Schedule(new ActionJob { Body = () => throw sameException });
        var b = scheduler.Schedule(new ActionJob { Body = () => throw sameException });
        var error = Failure(JobHandle.CombineDependencies(a, b));
        Check(error.InnerExceptions.Count == 2, "Independent failures must not be collapsed by exception identity.");
    }

    private static void ParallelFailureDrainsAllBatches()
    {
        using var scheduler = new JobScheduler(new() { WorkerCount = 4 });
        var counts = new int[128];
        var handle = scheduler.ScheduleParallel(new IndexJob { Body = i =>
        {
            Interlocked.Increment(ref counts[i]);
            if (i == 0 || i == 64) throw new Exception("batch failure");
        } }, counts.Length, 1);
        Check(Failure(handle).InnerExceptions.Count == 2, "Lost parallel failures.");
        foreach (int count in counts) Check(count == 1, "A batch was lost or repeated.");
        Check(handle.Status == JobStatus.Faulted, "Parallel failure must reach a terminal state.");
    }

    private static void DependenciesAndDisposal()
    {
        var scheduler = new JobScheduler(new() { WorkerCount = 2 });
        int value = 0;
        var first = scheduler.Schedule(new ActionJob { Body = () => value = 42 });
        var empty = scheduler.ScheduleParallel(new IndexJob(), 0, 1, first);
        var last = scheduler.Schedule(new ActionJob { Body = () =>
        {
            first.Complete(); // worker 可以观察已经完成的句柄。
            Check(value == 42, "Dependency result was not visible.");
        } }, empty);
        scheduler.Dispose();
        scheduler.Dispose();
        Check(first.IsCompleted && empty.IsCompleted && last.IsCompleted, "Dispose returned before jobs finished.");
        last.Complete();
        Check(last.Status == JobStatus.Succeeded, "Dispose did not drain jobs.");
        try { scheduler.Schedule(new ActionJob()); }
        catch (ObjectDisposedException) { return; }
        throw new Exception("Disposed scheduler accepted work.");
    }
}
