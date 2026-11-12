using System;
using MiniJob;

public static class Program
{
    private struct SquareJob : IJobFor
    {
        public int[] Values;

        public void Execute(int index)
        {
            Values[index] *= Values[index];
        }
    }

    public static void Main(string[] args)
    {
        if (args.Length == 1 && args[0] == "--test")
        {
            JobSchedulerTests.Run();
            return;
        }
        if (args.Length > 0 && args[0] == "--benchmark")
        {
            JobBenchmarks.Run(args);
            return;
        }

        using var scheduler = new JobScheduler();
        int[] values = { 1, 2, 3, 4 };

        // 提交 Job，每批处理 2 个元素。
        var handle = scheduler.ScheduleParallel(new SquareJob { Values = values }, values.Length, batchSize: 2);

        // 等待所有批次执行完毕，再读取结果。
        handle.Complete();
        Console.WriteLine(string.Join(", ", values));
    }
}
