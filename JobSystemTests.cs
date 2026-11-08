#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

/// <summary>Dependency-free regression runner: dotnet run -c Release -- --test.</summary>
internal static class JobSystemTests
{
    internal static void RunAll()
    {
        using var watchdog = new Timer(_ =>
        {
            Console.Error.WriteLine("FAIL: suite exceeded 60 seconds (possible scheduler deadlock).");
            Environment.Exit(1);
        }, null, TimeSpan.FromSeconds(60), Timeout.InfiniteTimeSpan);
        (string Name, Action Body)[] tests =
        {
            ("default handle", DefaultHandle),
            ("dependency fan-out/fan-in and duplicate edges", Dependencies),
            ("independent jobs really overlap", ParallelExecution),
            ("dynamic batches execute every index exactly once", ParallelRanges),
            ("empty range preserves dependency and failure", EmptyRange),
            ("struct job snapshot and inline Run", StructJobs),
            ("IJobFor serial and parallel / IJobParallelFor", TypedLoops),
            ("deferred jobs flush and Complete flushes", Deferred),
            ("Complete helps execute its graph", Helping),
            ("main-thread continuation affinity", MainThread),
            ("failures aggregate and skip descendants", Failures),
            ("CompleteAll still waits after a failure", CompleteAllFailure),
            ("nested blocking rejected / submission supported", NestedSubmission),
            ("cross-system handles rejected", ForeignHandles),
            ("concurrent submission and multiple completers", ConcurrentClients),
            ("buffer ownership lasts until Complete", BufferOwnership),
            ("read sharing / transitive write dependency", BufferDependencies),
            ("buffer scheduling validation is transactional", BufferTransaction),
            ("undeclared / readonly / parallel range checks", BufferChecks),
            ("buffer conflicts across schedulers", CrossSystemBuffers),
            ("Dispose drains deferred and main-thread work", Disposal),
            ("concurrent submission during shutdown", ConcurrentShutdown),
            ("argument validation", Arguments)
        };
        foreach (var (name, body) in tests)
        {
            body();
            Console.WriteLine($"PASS: {name}");
        }
        Console.WriteLine($"All {tests.Length} tests passed.");
    }

    private static void Check(bool condition, string message = "Assertion failed")
    {
        if (!condition) throw new Exception(message);
    }

    private static T Throws<T>(Action action) where T : Exception
    {
        try { action(); }
        catch (T error) { return error; }
        throw new Exception($"Expected {typeof(T).Name}");
    }

    private static void Wait(ManualResetEventSlim signal)
        => Check(signal.Wait(TimeSpan.FromSeconds(5)), "Timed out waiting for worker");

    private static void DefaultHandle()
    {
        JobHandle handle = default;
        Check(handle.IsCompleted);
        handle.Complete();
        Check(JobHandle.CombineDependencies(default, default) == default);
        JobHandle.CompleteAll(default, default);
    }

    private static void Dependencies()
    {
        foreach (int workers in new[] { 1, 2, 4 })
        {
            using var jobs = new JobSystem(workers);
            for (int repeat = 0; repeat < 100; repeat++)
            {
                int a = 0, b = 0, c = 0, d = 0;
                JobHandle first = jobs.Schedule(() => a = 10);
                JobHandle left = jobs.Schedule(() => b = a + 1, first, first, default);
                JobHandle right = jobs.Schedule(() => c = a + 2, first);
                JobHandle end = jobs.Schedule(() => d = b + c, JobHandle.CombineDependencies(left, right));
                end.Complete();
                Check(d == 23 && first.IsCompleted);
                end.Complete();
                jobs.Schedule(() => Check(d == 23), end).Complete();
            }
        }
    }

    private static void ParallelExecution()
    {
        using var jobs = new JobSystem(2);
        using var started = new CountdownEvent(2);
        using var release = new ManualResetEventSlim();
        var threads = new HashSet<int>();
        Action execute = () =>
        {
            lock (threads) threads.Add(Environment.CurrentManagedThreadId);
            started.Signal();
            Wait(release);
        };
        JobHandle a = jobs.Schedule(execute), b = jobs.Schedule(execute);
        try { Check(started.Wait(TimeSpan.FromSeconds(5))); }
        finally { release.Set(); }
        jobs.CompleteAll(a, b);
        Check(threads.Count == 2);
    }

    private static void ParallelRanges()
    {
        using var jobs = new JobSystem(4);
        foreach (int length in new[] { 0, 1, 17, 10003 })
        foreach (int batch in new[] { 1, 7, 1024, int.MaxValue })
        {
            var counts = new int[length];
            jobs.ParallelFor(length, batch, i => Interlocked.Increment(ref counts[i])).Complete();
            Check(counts.All(value => value == 1));
        }
    }

    private static void EmptyRange()
    {
        using var jobs = new JobSystem(1);
        int called = 0;
        JobHandle first = jobs.Schedule(() => called++);
        jobs.ParallelFor(0, 1, _ => throw new Exception(), first).Complete();
        Check(called == 1);
        JobHandle failed = jobs.Schedule(() => throw new InvalidOperationException("empty dependency"));
        Throws<InvalidOperationException>(() => jobs.ParallelFor(0, 1, _ => { }, failed).Complete());
    }

    private struct ValueJob : IJob
    {
        public int Value;
        public int[] Output;
        public void Execute() { Output[0] = Value; Value++; }
    }

    private static void StructJobs()
    {
        using var jobs = new JobSystem(1, deferScheduling: true);
        int[] output = new int[1];
        var job = new ValueJob { Value = 7, Output = output };
        JobHandle handle = job.Schedule(jobs);
        job.Value = 9;
        handle.Complete();
        Check(output[0] == 7 && job.Value == 9);
        job.Run(jobs);
        Check(output[0] == 9 && job.Value == 9);
        int caller = Environment.CurrentManagedThreadId;
        jobs.Run(() => Check(Environment.CurrentManagedThreadId == caller));
    }

    private struct LoopJob : IJobParallelFor
    {
        public int[] Counts;
        public void Execute(int index) => Interlocked.Increment(ref Counts[index]);
    }

    private static void TypedLoops()
    {
        using var jobs = new JobSystem(3);
        var loop = new LoopJob { Counts = new int[101] };
        loop.Schedule(jobs, 101).Complete();
        loop.ScheduleParallel(jobs, 101, 7).Complete();
        loop.Schedule(jobs, 101, 8).Complete();
        loop.Run(jobs, 101);
        Check(loop.Counts.All(n => n == 4));
    }

    private static void Deferred()
    {
        using var jobs = new JobSystem(1, deferScheduling: true);
        using var started = new ManualResetEventSlim();
        JobHandle handle = jobs.Schedule(started.Set);
        Check(!handle.IsCompleted && !started.IsSet);
        jobs.ScheduleBatchedJobs();
        Wait(started);
        handle.Complete();
        int value = 0;
        jobs.Schedule(() => value = 3).Complete();
        Check(value == 3);
    }

    private static void Helping()
    {
        using var jobs = new JobSystem(1);
        using var started = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        JobHandle blocked = jobs.Schedule(() => { started.Set(); Wait(release); });
        Wait(started);
        try
        {
            int caller = Environment.CurrentManagedThreadId;
            jobs.Schedule(() => Check(Environment.CurrentManagedThreadId == caller)).Complete();
        }
        finally { release.Set(); }
        blocked.Complete();
    }

    private static void MainThread()
    {
        using var jobs = new JobSystem(2);
        int owner = Environment.CurrentManagedThreadId, count = 0;
        JobHandle background = jobs.Schedule(() => Interlocked.Increment(ref count));
        JobHandle apply = jobs.ScheduleMainThread(() =>
        {
            Check(Environment.CurrentManagedThreadId == owner && count == 1);
            count++;
        }, background);
        jobs.Schedule(() => Check(count == 2), apply).Complete();
        JobHandle pumped = jobs.ScheduleMainThread(() => count++);
        Check(jobs.PumpMainThread() == 1);
        pumped.Complete();
        Check(count == 3);
        using var buffer = new JobBuffer<int>(1);
        JobHandle fill = jobs.Schedule(() => buffer[0] = 7, new[] { buffer.WriteOnly });
        jobs.ScheduleMainThread(() => Check(buffer[0] == 7), new[] { buffer.ReadOnly }, fill).Complete();
        Task.Run(() => Throws<InvalidOperationException>(() => jobs.PumpMainThread())).GetAwaiter().GetResult();
    }

    private static void Failures()
    {
        using var jobs = new JobSystem(2, deferScheduling: true);
        JobHandle a = jobs.Schedule(() => throw new InvalidOperationException("A"));
        JobHandle b = jobs.Schedule(() => throw new ArgumentException("B"));
        bool executed = false;
        JobHandle c = jobs.Schedule(() => executed = true, a, b);
        AggregateException error = Throws<AggregateException>(c.Complete);
        Check(error.InnerExceptions.Count == 2 && !executed && c.IsCompleted);
        Throws<AggregateException>(c.Complete);
        jobs.Schedule(() => executed = true).Complete();
        Check(executed);
    }

    private static void CompleteAllFailure()
    {
        using var jobs = new JobSystem(1, deferScheduling: true);
        int value = 0;
        JobHandle a = jobs.Schedule(() => throw new InvalidOperationException());
        JobHandle b = jobs.Schedule(() => value = 4);
        Throws<AggregateException>(() => JobHandle.CompleteAll(a, b));
        Check(value == 4 && b.IsCompleted);
    }

    private static void NestedSubmission()
    {
        using var jobs = new JobSystem(1);
        JobHandle child = default;
        int value = 0;
        jobs.Schedule(() =>
        {
            child = jobs.Schedule(() => value++);
            Throws<InvalidOperationException>(child.Complete);
            Throws<InvalidOperationException>(() => jobs.Run(() => { }));
            Throws<InvalidOperationException>(jobs.Dispose);
        }).Complete();
        child.Complete();
        Check(value == 1);
    }

    private static void ForeignHandles()
    {
        using var a = new JobSystem(1);
        using var b = new JobSystem(1);
        JobHandle handle = a.Schedule(() => { });
        Throws<ArgumentException>(() => b.Schedule(() => { }, handle));
        Throws<ArgumentException>(() => b.Complete(handle));
        Throws<ArgumentException>(() => b.IsCompleted(handle));
        handle.Complete();
    }

    private static void ConcurrentClients()
    {
        using var jobs = new JobSystem(4);
        int count = 0;
        var handles = new JobHandle[400];
        Parallel.For(0, handles.Length, i => handles[i] = jobs.Schedule(() => Interlocked.Increment(ref count)));
        JobHandle all = jobs.CombineDependencies(handles);
        Task[] waiters = Enumerable.Range(0, 8).Select(_ => Task.Run(all.Complete)).ToArray();
        Check(Task.WaitAll(waiters, TimeSpan.FromSeconds(10)));
        Check(count == handles.Length);
    }

    private static void BufferOwnership()
    {
        using var jobs = new JobSystem(2);
        using var buffer = new JobBuffer<int>(1);
        JobHandle handle = jobs.Schedule(() => buffer[0] = 42, new[] { buffer.ReadWrite });
        Check(SpinWait.SpinUntil(() => handle.IsCompleted, TimeSpan.FromSeconds(5)));
        Throws<InvalidOperationException>(() => { _ = buffer[0]; });
        Throws<InvalidOperationException>(buffer.Dispose);
        handle.Complete();
        Check(buffer[0] == 42);
        buffer.Dispose();
        Throws<ObjectDisposedException>(() => { _ = buffer[0]; });
    }

    private static void BufferDependencies()
    {
        using var jobs = new JobSystem(3, deferScheduling: true);
        using var buffer = new JobBuffer<int>(1);
        buffer[0] = 10;
        JobHandle a = jobs.Schedule(() => Check(buffer[0] == 10), new[] { buffer.ReadOnly });
        JobHandle b = jobs.Schedule(() => Check(buffer[0] == 10), new[] { buffer.ReadOnly });
        Check(buffer[0] == 10);
        Throws<InvalidOperationException>(() => buffer[0] = 2);
        Throws<InvalidOperationException>(() => jobs.Schedule(() => buffer[0]++, new[] { buffer.ReadWrite }, a));
        JobHandle barrier = jobs.CombineDependencies(a, b);
        JobHandle writer = jobs.Schedule(() => buffer[0]++, new[] { buffer.ReadWrite }, barrier);
        writer.Complete();
        Check(buffer[0] == 11);
        // Synchronizing an ancestor must not release a still-pending descendant's ownership.
        JobHandle first = jobs.Schedule(() => buffer[0]++, new[] { buffer.ReadWrite });
        JobHandle next = jobs.Schedule(() => buffer[0]++, new[] { buffer.ReadWrite }, first);
        first.Complete();
        Throws<InvalidOperationException>(() => { _ = buffer[0]; });
        next.Complete();
        Check(buffer[0] == 13);
    }

    private static void BufferTransaction()
    {
        using var jobs = new JobSystem(1, deferScheduling: true);
        using var a = new JobBuffer<int>(1);
        using var b = new JobBuffer<int>(1);
        JobHandle held = jobs.Schedule(() => b[0]++, new[] { b.ReadWrite });
        Throws<InvalidOperationException>(() => jobs.Schedule(() => { }, new[] { a.ReadWrite, b.ReadWrite }));
        a[0] = 12;
        Check(a[0] == 12);
        held.Complete();
    }

    private struct BufferLoop : IJobFor, IJobResources
    {
        public JobBuffer<int> Buffer;
        public void Execute(int index) => Buffer[index]++;
        public JobAccess[] GetAccesses() => new[] { Buffer.ReadWrite };
    }

    private static void BufferChecks()
    {
        using var jobs = new JobSystem(2);
        using var buffer = new JobBuffer<int>(20);
        new BufferLoop { Buffer = buffer }.ScheduleParallel(jobs, 20, 3).Complete();
        for (int i = 0; i < 20; i++) Check(buffer[i] == 1);
        Throws<InvalidOperationException>(() => jobs.Schedule(() => { _ = buffer[0]; }).Complete());
        Throws<InvalidOperationException>(() => jobs.Schedule(() => buffer[0] = 1, new[] { buffer.ReadOnly }).Complete());
        Throws<InvalidOperationException>(() => jobs.Schedule(() => { _ = buffer[0]; }, new[] { buffer.WriteOnly }).Complete());
        Throws<InvalidOperationException>(() => jobs.ParallelFor(1, 1, _ => buffer[1] = 2, new[] { buffer.ReadWrite }).Complete());
        Throws<IndexOutOfRangeException>(() => { _ = buffer[20]; });
    }

    private static void CrossSystemBuffers()
    {
        using var a = new JobSystem(1);
        using var b = new JobSystem(1);
        using var buffer = new JobBuffer<int>(1);
        JobHandle held = a.Schedule(() => buffer[0]++, new[] { buffer.ReadWrite });
        Throws<InvalidOperationException>(() => b.Schedule(() => buffer[0]++, new[] { buffer.ReadWrite }));
        held.Complete();
        b.Schedule(() => buffer[0]++, new[] { buffer.ReadWrite }).Complete();
        Check(buffer[0] == 2);
    }

    private static void Disposal()
    {
        var jobs = new JobSystem(2, deferScheduling: true);
        using var buffer = new JobBuffer<int>(1);
        JobHandle a = jobs.Schedule(() => buffer[0] = 8, new[] { buffer.ReadWrite });
        bool applied = false;
        JobHandle b = jobs.ScheduleMainThread(() => applied = true, a);
        jobs.Dispose();
        Check(a.IsCompleted && b.IsCompleted && buffer[0] == 8 && applied);
        jobs.Dispose();
        a.Complete();
        Throws<ObjectDisposedException>(() => jobs.Schedule(() => { }));
        Throws<ObjectDisposedException>(jobs.ScheduleBatchedJobs);
    }

    private static void Arguments()
    {
        Throws<ArgumentOutOfRangeException>(() => new JobSystem(-1));
        using var jobs = new JobSystem(1);
        Throws<ArgumentNullException>(() => jobs.Schedule((Action)null!));
        Throws<ArgumentNullException>(() => jobs.Schedule(() => { }, (JobHandle[])null!));
        Throws<ArgumentOutOfRangeException>(() => jobs.ParallelFor(-1, 1, _ => { }));
        Throws<ArgumentOutOfRangeException>(() => jobs.ParallelFor(1, 0, _ => { }));
        Throws<ArgumentException>(() => jobs.Schedule(() => { }, new JobAccess[] { default }));
        using var buffer = new JobBuffer<int>(1);
        buffer.Dispose();
        Throws<ObjectDisposedException>(() => jobs.Schedule(() => { }, new[] { buffer.ReadOnly }));
    }

    private static void ConcurrentShutdown()
    {
        var jobs = new JobSystem(2);
        using var started = new ManualResetEventSlim();
        int accepted = 0, executed = 0;
        Task producer = Task.Run(() =>
        {
            for (int i = 0; i < 1000; i++)
            {
                try { jobs.Schedule(() => Interlocked.Increment(ref executed)); }
                catch (ObjectDisposedException) { break; }
                accepted++;
                started.Set();
            }
        });
        Wait(started);
        jobs.Dispose();
        producer.GetAwaiter().GetResult();
        Check(accepted == executed && accepted > 0);
    }
}
