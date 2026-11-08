public interface IJob { void Execute(); }
public interface IJobFor { void Execute(int index); }
public interface IJobParallelFor : IJobFor { }

/// <summary>Declare every JobBuffer read/write by a struct job. Arbitrary fields are not reflected.</summary>
public interface IJobResources { JobAccess[] GetAccesses(); }

public static class JobExtensions
{
    public static JobHandle Schedule<T>(this T job, JobSystem jobs, params JobHandle[] dependencies)
        where T : struct, IJob => jobs.Schedule(job, dependencies);

    public static JobHandle Schedule<T>(this T job, JobSystem jobs, int length, params JobHandle[] dependencies)
        where T : struct, IJobFor => jobs.Schedule(job, length, dependencies);

    public static JobHandle Schedule<T>(this T job, JobSystem jobs, int length, int batchSize,
        params JobHandle[] dependencies) where T : struct, IJobParallelFor
        => jobs.Schedule(job, length, batchSize, dependencies);

    public static JobHandle ScheduleParallel<T>(this T job, JobSystem jobs, int length, int batchSize,
        params JobHandle[] dependencies) where T : struct, IJobFor
        => jobs.ScheduleParallel(job, length, batchSize, dependencies);

    public static void Run<T>(this T job, JobSystem jobs, params JobHandle[] dependencies)
        where T : struct, IJob => jobs.Run(job, dependencies);

    public static void Run<T>(this T job, JobSystem jobs, int length, params JobHandle[] dependencies)
        where T : struct, IJobFor => jobs.Run(job, length, dependencies);
}
