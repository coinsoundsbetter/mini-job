using System;

namespace MiniJob;

public sealed class JobSchedulerOptions
{
    public int WorkerCount { get; init; } = Math.Max(1, Environment.ProcessorCount - 1);
}
