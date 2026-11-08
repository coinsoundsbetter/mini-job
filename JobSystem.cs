#nullable enable
using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;

/// <summary>A CPU job scheduler. Dependencies order work; they do not lock arbitrary user data.</summary>
public sealed class JobSystem : IDisposable
{
    private readonly object _gate = new();
    private readonly LinkedList<WorkItem> _ready = new();
    private readonly HashSet<JobNode> _tracked = new();
    private readonly Thread[] _workers;
    private readonly int _ownerThread = Environment.CurrentManagedThreadId;
    private readonly bool _deferScheduling;
    private bool _closing;
    private int _outstanding;

    public int WorkerCount => _workers.Length;

    public JobSystem(int workerCount = 0, bool deferScheduling = false)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(workerCount);
        if (workerCount == 0) workerCount = Math.Max(1, Environment.ProcessorCount - 1);
        _deferScheduling = deferScheduling;
        _workers = new Thread[workerCount];
        int started = 0;
        try
        {
            for (; started < workerCount; started++)
            {
                _workers[started] = new Thread(WorkerLoop)
                {
                    Name = $"MiniJob Worker {started}", IsBackground = true
                };
                _workers[started].Start();
            }
        }
        catch
        {
            lock (_gate) { _closing = true; Monitor.PulseAll(_gate); }
            for (int i = 0; i < started; i++) _workers[i].Join();
            throw;
        }
    }

    public JobHandle Schedule(Action execute, params JobHandle[] dependencies)
        => Schedule(execute, Array.Empty<JobAccess>(), dependencies);

    public JobHandle Schedule(Action execute, JobAccess[] accesses, params JobHandle[] dependencies)
    {
        ArgumentNullException.ThrowIfNull(execute);
        return Submit(new[] { execute }, accesses, dependencies);
    }

    public JobHandle Schedule<T>(T job, params JobHandle[] dependencies) where T : struct, IJob
        => Schedule(() => job.Execute(), GetAccesses(job), dependencies);

    public JobHandle Schedule<T>(T job, int length, params JobHandle[] dependencies) where T : struct, IJobFor
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        return Schedule(() => { for (int i = 0; i < length; i++) job.Execute(i); }, GetAccesses(job), dependencies);
    }

    public JobHandle ParallelFor(int length, int batchSize, Action<int> execute, params JobHandle[] dependencies)
        => ParallelFor(length, batchSize, execute, Array.Empty<JobAccess>(), dependencies);

    public JobHandle ParallelFor(int length, int batchSize, Action<int> execute, JobAccess[] accesses,
        params JobHandle[] dependencies)
    {
        ArgumentNullException.ThrowIfNull(execute);
        return ParallelCore(length, batchSize, () => execute, accesses, dependencies);
    }

    public JobHandle ScheduleParallel<T>(T job, int length, int batchSize, params JobHandle[] dependencies)
        where T : struct, IJobFor
        => ParallelCore(length, batchSize, () => { T copy = job; return index => copy.Execute(index); },
            GetAccesses(job), dependencies);

    public JobHandle Schedule<T>(T job, int length, int batchSize, params JobHandle[] dependencies)
        where T : struct, IJobParallelFor
        => ParallelCore(length, batchSize, () => { T copy = job; return index => copy.Execute(index); },
            GetAccesses(job), dependencies);

    private JobHandle ParallelCore(int length, int batchSize, Func<Action<int>> createExecute,
        JobAccess[] accesses, JobHandle[] dependencies)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        // Only O(worker count) work items, even when batchSize == 1. A long cursor avoids int overflow.
        long cursor = 0;
        int runners = length == 0 ? 1 : Math.Min(WorkerCount, (length - 1) / batchSize + 1);
        var actions = new Action[runners];
        for (int runner = 0; runner < runners; runner++)
        {
            Action<int> execute = createExecute();
            actions[runner] = () =>
            {
                while (true)
                {
                    long first = Interlocked.Add(ref cursor, batchSize) - batchSize;
                    if (first >= length) return;
                    int end = (int)Math.Min(first + batchSize, length);
                    for (int i = (int)first; i < end; i++)
                    {
                        JobExecution.Index = i;
                        execute(i);
                    }
                }
            };
        }
        return Submit(actions, accesses, dependencies);
    }

    public void Run<T>(T job, params JobHandle[] dependencies) where T : struct, IJob
        => Run(() => job.Execute(), GetAccesses(job), dependencies);

    public void Run<T>(T job, int length, params JobHandle[] dependencies) where T : struct, IJobFor
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        Run(() => { for (int i = 0; i < length; i++) job.Execute(i); }, GetAccesses(job), dependencies);
    }

    public void Run(Action execute, params JobHandle[] dependencies)
        => Run(execute, Array.Empty<JobAccess>(), dependencies);

    public void Run(Action execute, JobAccess[] accesses, params JobHandle[] dependencies)
    {
        ArgumentNullException.ThrowIfNull(execute);
        RejectJobWait();
        CompleteAll(dependencies);
        JobHandle handle = Submit(new[] { execute }, accesses, Array.Empty<JobHandle>(), inline: true);
        Execute(new WorkItem(handle.Node!, execute));
        Complete(handle);
    }

    /// <summary>Runs on the thread that constructed this system, when it pumps or completes jobs.</summary>
    public JobHandle ScheduleMainThread(Action execute, params JobHandle[] dependencies)
        => ScheduleMainThread(execute, Array.Empty<JobAccess>(), dependencies);

    public JobHandle ScheduleMainThread(Action execute, JobAccess[] accesses, params JobHandle[] dependencies)
    {
        ArgumentNullException.ThrowIfNull(execute);
        return Submit(new[] { execute }, accesses, dependencies, mainThread: true);
    }

    public int PumpMainThread(int maxJobs = int.MaxValue)
    {
        EnsureOwnerThread();
        RejectJobWait();
        ArgumentOutOfRangeException.ThrowIfNegative(maxJobs);
        ScheduleBatchedJobs();
        int count = 0;
        while (count < maxJobs)
        {
            WorkItem? work;
            lock (_gate) work = TakeReady(null, mainOnly: true);
            if (work is null) break;
            Execute(work);
            count++;
        }
        return count;
    }

    public JobHandle CombineDependencies(params JobHandle[] dependencies)
        => Submit(new Action[] { static () => { } }, Array.Empty<JobAccess>(), dependencies);

    public void CompleteAll(params JobHandle[] handles)
    {
        RejectJobWait();
        JobHandle[] copy = CopyDependencies(handles);
        var failures = new List<Exception>();
        foreach (JobHandle handle in copy)
        {
            try { Complete(handle); }
            catch (Exception error) { failures.Add(error); }
        }
        if (failures.Count != 0) throw new AggregateException(failures).Flatten();
    }

    public void Complete(JobHandle handle)
    {
        RejectJobWait();
        ValidateHandle(handle);
        if (handle.Node is not { } target) return;
        HashSet<JobNode> ancestors;
        lock (_gate)
        {
            FlushLocked();
            ancestors = CollectAncestors(target);
        }
        while (true)
        {
            WorkItem? work;
            lock (_gate)
            {
                if (target.Finished) break;
                // Only help this dependency graph; never run unrelated work while waiting.
                work = TakeReady(ancestors);
                if (work is null) { Monitor.Wait(_gate); continue; }
            }
            Execute(work);
        }
        lock (_gate)
        {
            foreach (JobNode node in ancestors)
            {
                JobSafety.Release(node);
                _tracked.Remove(node);
                node.Parents.Clear();
            }
        }
        ThrowFailures(target);
    }

    public bool IsCompleted(JobHandle handle)
    {
        ValidateHandle(handle);
        return handle.IsCompleted;
    }

    public void ScheduleBatchedJobs()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closing, this);
            FlushLocked();
        }
    }

    public void Dispose()
    {
        RejectJobWait();
        // Owner affinity guarantees queued main-thread continuations can be drained.
        EnsureOwnerThread();
        lock (_gate) { _closing = true; FlushLocked(); }
        while (true)
        {
            WorkItem? work;
            lock (_gate)
            {
                if (_outstanding == 0) break;
                work = TakeReady(null);
                if (work is null) { Monitor.Wait(_gate); continue; }
            }
            Execute(work);
        }
        foreach (Thread worker in _workers) worker.Join();
        lock (_gate)
        {
            foreach (JobNode node in _tracked)
            {
                JobSafety.Release(node);
                node.Parents.Clear();
            }
            _tracked.Clear();
        }
    }

    private JobHandle Submit(Action[] actions, JobAccess[] accesses, JobHandle[] dependencies,
        bool mainThread = false, bool inline = false)
    {
        JobHandle[] copy = CopyDependencies(dependencies);
        ArgumentNullException.ThrowIfNull(accesses);
        accesses = (JobAccess[])accesses.Clone();
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closing, this);
            var node = new JobNode(this, actions, accesses, mainThread, inline || !_deferScheduling);
            foreach (JobHandle dependency in copy)
                if (dependency.Node is { } parent) node.Parents.Add(parent);
            // Validate all resources before mutating queues or dependency lists.
            JobSafety.Reserve(node);
            foreach (JobNode parent in node.Parents)
            {
                if (parent.Finished) node.Failures.UnionWith(parent.Failures);
                else { parent.Dependents.Add(node); node.RemainingDependencies++; }
            }
            _tracked.Add(node);
            _outstanding++;
            if (!inline && node.RemainingDependencies == 0 && node.Released) Enqueue(node);
            return new JobHandle(node);
        }
    }

    private void Enqueue(JobNode node)
    {
        foreach (Action action in node.Actions!) _ready.AddLast(new WorkItem(node, action));
        node.Actions = null;
        Monitor.PulseAll(_gate);
    }

    private void FlushLocked()
    {
        foreach (JobNode node in _tracked)
        {
            if (node.Released) continue;
            node.Released = true;
            if (node.RemainingDependencies == 0) Enqueue(node);
        }
        Monitor.PulseAll(_gate);
    }

    private WorkItem? TakeReady(HashSet<JobNode>? allowed, bool mainOnly = false)
    {
        bool owner = Environment.CurrentManagedThreadId == _ownerThread;
        for (var link = _ready.First; link is not null; link = link.Next)
        {
            WorkItem work = link.Value;
            if (work.Node.MainThread && !owner || mainOnly && !work.Node.MainThread) continue;
            if (allowed is not null && !allowed.Contains(work.Node)) continue;
            _ready.Remove(link);
            return work;
        }
        return null;
    }

    private void WorkerLoop()
    {
        while (true)
        {
            WorkItem? work;
            lock (_gate)
            {
                while ((work = TakeReady(null)) is null)
                {
                    if (_closing && _outstanding == 0) return;
                    Monitor.Wait(_gate);
                }
            }
            Execute(work);
        }
    }

    private void Execute(WorkItem work)
    {
        JobNode node = work.Node;
        bool skip;
        lock (_gate) skip = node.Failures.Count != 0;
        Exception? failure = null;
        JobExecution.Current = node;
        JobExecution.Index = -1;
        try { if (!skip) work.Execute(); }
        catch (Exception error) { failure = error; }
        finally { JobExecution.Current = null; JobExecution.Index = -1; }
        lock (_gate)
        {
            if (failure is not null) node.Failures.Add(failure);
            if (--node.RemainingWork == 0)
            {
                node.Actions = null;
                Volatile.Write(ref node.FinishedFlag, 1);
                _outstanding--;
                foreach (JobNode dependent in node.Dependents)
                {
                    dependent.Failures.UnionWith(node.Failures);
                    if (--dependent.RemainingDependencies == 0 && dependent.Released) Enqueue(dependent);
                }
                node.Dependents.Clear();
            }
            Monitor.PulseAll(_gate);
        }
    }

    internal static HashSet<JobNode> CollectAncestors(JobNode root)
    {
        var result = new HashSet<JobNode>();
        var pending = new Stack<JobNode>();
        pending.Push(root);
        while (pending.TryPop(out JobNode? node))
            if (result.Add(node)) foreach (JobNode parent in node.Parents) pending.Push(parent);
        return result;
    }

    private JobHandle[] CopyDependencies(JobHandle[] dependencies)
    {
        ArgumentNullException.ThrowIfNull(dependencies);
        var copy = (JobHandle[])dependencies.Clone();
        foreach (JobHandle handle in copy) ValidateHandle(handle);
        return copy;
    }

    private void ValidateHandle(JobHandle handle)
    {
        if (handle.Node is { } node && node.Owner != this)
            throw new ArgumentException("The handle belongs to another JobSystem.", nameof(handle));
    }

    private static JobAccess[] GetAccesses<T>(T job) where T : struct
        => job is IJobResources resources ? resources.GetAccesses() : Array.Empty<JobAccess>();

    private static void RejectJobWait()
    {
        if (JobExecution.Current is not null)
            throw new InvalidOperationException("Jobs cannot wait, run synchronously, pump, or dispose a scheduler. Express ordering with dependencies.");
    }

    private void EnsureOwnerThread()
    {
        if (Environment.CurrentManagedThreadId != _ownerThread)
            throw new InvalidOperationException("This operation must run on the thread that created the JobSystem.");
    }

    private static void ThrowFailures(JobNode node)
    {
        if (node.Failures.Count == 1)
            foreach (Exception error in node.Failures) ExceptionDispatchInfo.Capture(error).Throw();
        if (node.Failures.Count > 1) throw new AggregateException(node.Failures).Flatten();
    }
}

/// <summary>A default handle is already complete. IsCompleted does not release buffer ownership; Complete does.</summary>
public readonly struct JobHandle : IEquatable<JobHandle>
{
    internal JobNode? Node { get; }
    internal JobHandle(JobNode node) => Node = node;
    public bool IsCompleted => Node is null || Node.Finished;
    public void Complete() => Node?.Owner.Complete(this);
    public bool Equals(JobHandle other) => ReferenceEquals(Node, other.Node);
    public override bool Equals(object? obj) => obj is JobHandle other && Equals(other);
    public override int GetHashCode() => Node?.GetHashCode() ?? 0;
    public static bool operator ==(JobHandle left, JobHandle right) => left.Equals(right);
    public static bool operator !=(JobHandle left, JobHandle right) => !left.Equals(right);

    public static JobHandle CombineDependencies(params JobHandle[] handles)
    {
        ArgumentNullException.ThrowIfNull(handles);
        foreach (JobHandle handle in handles)
            if (handle.Node is { } node) return node.Owner.CombineDependencies(handles);
        return default;
    }

    public static void CompleteAll(params JobHandle[] handles)
    {
        ArgumentNullException.ThrowIfNull(handles);
        foreach (JobHandle handle in handles)
            if (handle.Node is { } node) { node.Owner.CompleteAll(handles); return; }
    }
}

internal sealed class JobNode
{
    internal readonly JobSystem Owner;
    internal readonly HashSet<JobNode> Parents = new();
    internal readonly List<JobNode> Dependents = new();
    internal readonly HashSet<Exception> Failures = new(ReferenceEqualityComparer.Instance);
    internal readonly JobAccess[] Accesses;
    internal readonly bool MainThread;
    internal Action[]? Actions;
    internal int RemainingDependencies;
    internal int RemainingWork;
    internal int FinishedFlag;
    internal bool Released;
    internal bool Finished => Volatile.Read(ref FinishedFlag) != 0;

    internal JobNode(JobSystem owner, Action[] actions, JobAccess[] accesses, bool mainThread, bool released)
    {
        Owner = owner; Actions = actions; Accesses = accesses; MainThread = mainThread;
        RemainingWork = actions.Length; Released = released;
    }
}

internal sealed record WorkItem(JobNode Node, Action Execute);

internal static class JobExecution
{
    [ThreadStatic] internal static JobNode? Current;
    [ThreadStatic] internal static int Index;
}
