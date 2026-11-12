using System;
using System.Collections.Generic;

namespace MiniJob;

internal sealed class JobNode
{
    internal readonly JobScheduler Owner;
    internal JobStatus Status = JobStatus.WaitingForDependencies;
    internal readonly List<JobNode> Dependents = new();
    internal readonly HashSet<JobFailure> Errors = new();
    internal int Dependencies, Remaining;
    internal Action<int> Body;
    internal int Length, BatchSize;
    internal JobNode(JobScheduler owner) => Owner = owner;
    internal bool Done => Status is JobStatus.Succeeded or JobStatus.Faulted or JobStatus.Skipped;
}

internal sealed class JobFailure(Exception error)
{
    internal readonly Exception Error = error;
}
