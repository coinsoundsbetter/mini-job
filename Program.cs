using System;

internal static class Program
{
    private static void Main(string[] args)
    {
        if (Array.IndexOf(args, "--test") >= 0)
        {
            JobSystemTests.RunAll();
            return;
        }
        const int particleCount = 100_000;
        const float deltaTime = 1f / 60f;
        using var positions = new JobBuffer<float>(particleCount);
        using var velocities = new JobBuffer<float>(particleCount);
        using var maximum = new JobBuffer<float>(1);
        for (int i = 0; i < particleCount; i++) velocities[i] = 60f;

        int workerCount = Math.Max(1, Environment.ProcessorCount - 1);
        using var jobs = new JobSystem(workerCount);

        JobHandle update = new MoveParticles
        {
            Positions = positions, Velocities = velocities, DeltaTime = deltaTime
        }.ScheduleParallel(jobs, particleCount, batchSize: 1024);

        JobHandle bounds = jobs.Schedule(() =>
        {
            float value = 0f;
            for (int i = 0; i < positions.Length; i++) value = Math.Max(value, positions[i]);
            maximum[0] = value;
        }, new[] { positions.ReadOnly, maximum.WriteOnly }, update);

        JobHandle apply = jobs.ScheduleMainThread(() =>
            Console.WriteLine($"Maximum position: {maximum[0]}"), new[] { maximum.ReadOnly }, bounds);

        // The main thread can do independent frame work before reaching this point.
        apply.Complete();
        for (int i = 0; i < positions.Length; i++)
        {
            if (Math.Abs(positions[i] - 1f) > 0.00001f)
                throw new InvalidOperationException("A particle was updated incorrectly.");
        }

        Console.WriteLine($"Workers: {workerCount}");
        Console.WriteLine($"Particles updated: {positions.Length:N0}");
    }

    private struct MoveParticles : IJobFor, IJobResources
    {
        public JobBuffer<float> Positions;
        public JobBuffer<float> Velocities;
        public float DeltaTime;
        public void Execute(int index) => Positions[index] += Velocities[index] * DeltaTime;
        public JobAccess[] GetAccesses() => new[] { Positions.ReadWrite, Velocities.ReadOnly };
    }
}
