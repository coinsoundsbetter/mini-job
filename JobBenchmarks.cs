using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using MiniJob;

public static class JobBenchmarks
{
    private struct ComputeJob : IJobFor
    {
        public uint[] Input;
        public uint[] Output;
        public int Iterations;
        public void Execute(int index) => Output[index] = Compute(Input[index], Iterations);
    }

    // 两种执行方式调用同一个计算函数，避免因算法不同影响对比。禁止内联，保持调用边界一致。
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static uint Compute(uint value, int iterations)
    {
        unchecked
        {
            for (int i = 0; i < iterations; i++)
            {
                value ^= value << 13;
                value ^= value >> 17;
                value ^= value << 5;
                value = value * 1664525u + 1013904223u;
            }
        }
        return value;
    }

    public static void Run(string[] args)
    {
        int workers = Math.Max(1, Environment.ProcessorCount - 1);
        if (args.Length > 2 || (args.Length == 2 &&
            (!int.TryParse(args[1], out workers) || workers <= 0)))
            throw new ArgumentException("用法：--benchmark [工作线程数，必须为正整数]");
#if DEBUG
        Console.WriteLine("提示：当前为 Debug 构建，性能测试请使用 dotnet run -c Release -- --benchmark");
#endif
        Console.WriteLine($"{RuntimeInformation.FrameworkDescription}; {RuntimeInformation.OSDescription}");
        Console.WriteLine($"逻辑处理器数量：{Environment.ProcessorCount}；工作线程数量：{workers}");
        Console.WriteLine("测量 7 轮，交替执行顺序；显示单次操作耗时的中位数 [最小值, 最大值]，单位为毫秒。");
        Console.WriteLine("计时包含任务提交和等待完成，不包含线程池创建、数组分配及结果校验。");
        Console.WriteLine("加速比 = 普通循环耗时 / Job 耗时；小于 1 表示使用 Job 更慢。\n");
        using var scheduler = new JobScheduler(new()
        {
            WorkerCount = workers
        });
        Measure(scheduler, "微小计算", 128, 8, 32);
        Measure(scheduler, "轻量计算", 100_000, 1, 256);
        Measure(scheduler, "密集计算", 100_000, 512, 256);
        Console.WriteLine("全部输出数组逐元素校验一致。实际耗时受处理器负载和计算任务影响。");
    }

    private static void Measure(
        JobScheduler scheduler, 
        string name, 
        int count, 
        int iterations, 
        int batchSize)
    {
        var input = new uint[count];
        var sequential = new uint[count];
        var parallel = new uint[count];
        for (int i = 0; i < count; i++)
            input[i] = (uint)(i + 1);
        var job = new ComputeJob { Input = input, Output = parallel, Iterations = iterations };
        Action direct = () =>
        {
            for (int i = 0; i < count; i++)
                sequential[i] = Compute(input[i], iterations);
        };
        Action scheduled = () => scheduler.ScheduleParallel(job, count, batchSize).Complete();
        // 先预热至少 300 毫秒，让线程启动和 JIT 编译尽量发生在正式计时之前。
        var warmup = Stopwatch.StartNew();
        do
        {
            direct();
            scheduled();
        } while (warmup.ElapsedMilliseconds < 300);
        Validate(sequential, parallel);
        // 根据串行耗时估算重复次数，两种方式使用相同次数，减小短任务的计时误差。
        double singleMs = Time(direct, 1);
        int repetitions = (int)Math.Clamp(Math.Ceiling(20 / Math.Max(singleMs, 0.001)), 1, 1000);
        var directTimes = new double[7];
        var jobTimes = new double[7];
        // 每轮交换先后顺序，降低固定执行顺序带来的偏差。
        for (int round = 0; round < directTimes.Length; round++)
        {
            if (round % 2 == 0)
            {
                directTimes[round] = Time(direct, repetitions);
                jobTimes[round] = Time(scheduled, repetitions);
            }
            else
            {
                jobTimes[round] = Time(scheduled, repetitions);
                directTimes[round] = Time(direct, repetitions);
            }
            Validate(sequential, parallel);
        }
        // 统计的是各轮单次平均耗时；排序后取第 4 项作为 7 轮的中位数。
        Array.Sort(directTimes);
        Array.Sort(jobTimes);
        Console.WriteLine($"{name}：元素数量={count:N0}，每元素计算轮数={iterations}，批大小={batchSize}，每轮重复次数={repetitions}");
        Console.WriteLine($"  普通循环：{directTimes[3]:F4} [{directTimes[0]:F4}, {directTimes[6]:F4}] 毫秒");
        Console.WriteLine($"  使用 Job：{jobTimes[3]:F4} [{jobTimes[0]:F4}, {jobTimes[6]:F4}] 毫秒");
        Console.WriteLine($"  加速比：{directTimes[3] / jobTimes[3]:F2} 倍\n");
    }

    private static double Time(Action action, int repetitions)
    {
        long start = Stopwatch.GetTimestamp();
        for (int i = 0; i < repetitions; i++)
            action();
        return Stopwatch.GetElapsedTime(start).TotalMilliseconds / repetitions;
    }
    // 校验不计入耗时，确保加速没有以错误的计算结果为代价。
    private static void Validate(uint[] expected, uint[] actual)
    {
        if (!expected.AsSpan().SequenceEqual(actual))
            throw new InvalidOperationException("普通循环与 Job 并行计算的结果不一致。");
    }
}

