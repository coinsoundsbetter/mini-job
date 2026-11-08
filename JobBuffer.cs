#nullable enable
using System;
using System.Collections.Generic;

[Flags]
internal enum AccessMode { Read = 1, Write = 2 }

/// <summary>An explicit access declaration, consumed when a job is scheduled.</summary>
public readonly struct JobAccess
{
    internal JobBufferBase Buffer { get; }
    internal AccessMode Mode { get; }
    internal JobAccess(JobBufferBase buffer, AccessMode mode) { Buffer = buffer; Mode = mode; }
}

public abstract class JobBufferBase
{
    internal readonly Dictionary<JobNode, AccessMode> Reservations = new();
    internal bool Disposed;
    internal JobBufferBase() { }
    public JobAccess ReadOnly => new(this, AccessMode.Read);
    public JobAccess WriteOnly => new(this, AccessMode.Write);
    public JobAccess ReadWrite => new(this, AccessMode.Read | AccessMode.Write);
}

/// <summary>
/// A managed, checked buffer, not native memory. Call Complete before reclaiming scheduled access.
/// Parallel jobs may access writable buffers only at their current Execute(index).
/// </summary>
public sealed class JobBuffer<T> : JobBufferBase, IDisposable where T : unmanaged
{
    private T[] _data;
    public int Length { get; }

    public JobBuffer(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        Length = length;
        _data = new T[length];
    }

    public T this[int index]
    {
        get
        {
            lock (JobSafety.Gate)
            {
                CheckAccess(index, AccessMode.Read);
                return _data[index];
            }
        }
        set
        {
            lock (JobSafety.Gate)
            {
                CheckAccess(index, AccessMode.Write);
                _data[index] = value;
            }
        }
    }

    public void Dispose()
    {
        lock (JobSafety.Gate)
        {
            if (Disposed) return;
            if (Reservations.Count != 0)
                throw new InvalidOperationException("Complete every job using this buffer before disposing it.");
            Disposed = true;
            _data = Array.Empty<T>();
        }
    }

    private void CheckAccess(int index, AccessMode requested)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        if ((uint)index >= (uint)Length) throw new IndexOutOfRangeException();
        if (JobExecution.Current is { } node)
        {
            if (!Reservations.TryGetValue(node, out AccessMode declared) || (declared & requested) == 0)
                throw new InvalidOperationException("The job did not declare the required buffer access.");
            if ((declared & AccessMode.Write) != 0 && JobExecution.Index >= 0 && index != JobExecution.Index)
                throw new InvalidOperationException("A parallel job can only access its own index in a writable buffer.");
            return;
        }
        foreach (AccessMode reserved in Reservations.Values)
            if ((requested & AccessMode.Write) != 0 || (reserved & AccessMode.Write) != 0)
                throw new InvalidOperationException("Call Complete before accessing this scheduled buffer.");
    }
}

internal static class JobSafety
{
    // Lock ordering: scheduler gate -> safety gate. Buffer accesses never take the scheduler gate.
    // Checks are always enabled and deliberately favor simplicity over throughput.
    internal static readonly object Gate = new();

    internal static void Reserve(JobNode node)
    {
        var combined = new Dictionary<JobBufferBase, AccessMode>();
        foreach (JobAccess access in node.Accesses)
        {
            if (access.Buffer is null) throw new ArgumentException("A default JobAccess is invalid.");
            combined.TryGetValue(access.Buffer, out AccessMode mode);
            combined[access.Buffer] = mode | access.Mode;
        }
        if (combined.Count == 0) return;
        HashSet<JobNode> ancestors = JobSystem.CollectAncestors(node);
        lock (Gate)
        {
            foreach (var (buffer, mode) in combined)
            {
                ObjectDisposedException.ThrowIf(buffer.Disposed, buffer);
                foreach (var (other, otherMode) in buffer.Reservations)
                    if (((mode | otherMode) & AccessMode.Write) != 0 && !ancestors.Contains(other))
                        throw new InvalidOperationException("Conflicting buffer access: supply the existing job as a dependency or Complete it first.");
            }
            foreach (var (buffer, mode) in combined) buffer.Reservations.Add(node, mode);
        }
    }

    internal static void Release(JobNode node)
    {
        lock (Gate)
            foreach (JobAccess access in node.Accesses) access.Buffer.Reservations.Remove(node);
    }
}
