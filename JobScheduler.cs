using System;
using System.Collections.Generic;
using System.Threading;

namespace MiniJob;

/// <summary>
/// 单调度器、全局锁版本
/// </summary>
public sealed class JobScheduler : IDisposable
{
    private readonly object gate = new();
    private bool closing;
    private bool stopped;
    private int unfinishedJobCount;
    private int nextQueue;
    private readonly LinkedList<WorkItem>[] queues;
    private readonly Thread[] workers;
    private readonly Thread submissionThread = Thread.CurrentThread;
    [ThreadStatic] private static JobScheduler currentScheduler;
    [ThreadStatic] private static int workerIndex;

    private readonly record struct WorkItem(JobNode Node, int Begin, int End);

    public JobScheduler(JobSchedulerOptions options = null)
    {
        int count = (options ?? new JobSchedulerOptions()).WorkerCount;
        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options));
        }

        queues = new LinkedList<WorkItem>[count];
        workers = new Thread[count];
        for (int i = 0; i < count; i++)
        {
            queues[i] = new();
        }
            
        for (int i = 0; i < count; i++)
        {
            int index = i;
            workers[i] = new Thread(() => WorkerLoop(index)) { IsBackground = true, Name = $"MiniJob-{i}" };
            workers[i].Start();
        }
    }
    
    public void Dispose()
    {
        //禁止工作线程越权关闭
        if (currentScheduler == this)
        {
            throw new InvalidOperationException("A worker cannot dispose its scheduler.");
        }
            
        //使用锁保证线程同步
        lock (gate)
        {
            //标记为开始关闭,将会停止后续的新任务提交
            closing = true;
            //当前还有未完成的任务
            while (unfinishedJobCount != 0)
            {
                //释放锁,让工作线程拿到并等待它们完成任务
                Monitor.Wait(gate);
                //在这里,主线程会重新拿到锁,并继续执行while条件检查
            }
            
            //唤醒正在休眠的工作线程(并不释放锁),这样它们会检测到stopped,从而关闭各个工作线程
            stopped = true;
            Monitor.PulseAll(gate);
        }

        //逐个等待各个工作线程结束
        foreach (var worker in workers)
        {
            //这是一个阻塞调用,会等待直到轮询的这个工作线程退出
            worker.Join();
        }
    }

    public JobHandle Schedule<T>(T job, JobHandle dependsOn = default) where T : struct, IJob =>
        Submit(_ => job.Execute(), 1, 1, new[] { dependsOn });

    public JobHandle ScheduleParallel<T>(T job, int length, int batchSize, JobHandle dependsOn = default) where T : struct, IJobFor
    {
        if (length < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(length));
        }

        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize));
        }
        
        return Submit(index =>
        {
            T copy = job;
            copy.Execute(index);
        }, length, batchSize, new[] { dependsOn });
    }

    public void Run<T>(T job) where T : struct, IJob => job.Execute();

    public void RunFor<T>(T job, int length) where T : struct, IJobFor
    {
        if (length < 0)
            throw new ArgumentOutOfRangeException(nameof(length));
        for (int i = 0; i < length; i++)
        {
            T copy = job;
            copy.Execute(i);
        }
    }

    internal JobHandle Combine(ReadOnlySpan<JobHandle> handles) => Submit(null, 0, 1, handles);

    private JobHandle Submit(Action<int> body, int length, int batchSize, ReadOnlySpan<JobHandle> dependencies)
    {
        //递交不一定是由主线程发起的,加锁防止竞态问题
        lock (gate)
        {
            //此时认为释放的优先级最高,所以不再接受任何的Job任务了
            if (closing)
            {
                throw new ObjectDisposedException(nameof(JobScheduler));
            }
            
            //只有调度线程(或者说是主线程)才可以提交调度任务
            if (Thread.CurrentThread != submissionThread)
            {
                throw new InvalidOperationException("Only the thread that created the scheduler can submit jobs.");
            }
                
            //收集此次任务的所有前置依赖
            var unique = new HashSet<JobNode>();
            foreach (var handle in dependencies)
            {
                if (handle.Node == null)
                {
                    continue;
                }
             
                //当前的依赖是否属于同一个调度器(理论上来说是肯定的,因为我们目前假设全局只有一个唯一的调度器)
                if (handle.Node.Owner != this)
                {
                    throw new ArgumentException("Foreign dependency.");
                }
                    
                unique.Add(handle.Node);
            }

            //为当前任务创建任务节点
            var node = new JobNode(this) { Body = body, Length = length, BatchSize = batchSize };
            foreach (var dependency in unique)
            {
                //依赖项已经结束了,我们需要检查它的结束原因是否源于失败
                if (dependency.Done)
                {
                    InheritFailure(node, dependency);
                }
                //否则,等待依赖项
                else
                {
                    node.Dependencies++;
                    dependency.Dependents.Add(node);
                }
            }

            //更新当前工作中的任务数
            unfinishedJobCount++;
            
            //经过上述检查,该任务不存在依赖项,可以直接进入待开始队列
            if (node.Dependencies == 0)
            {
                Activate(node);
            }
               
            return new JobHandle(node);
        }
    }

    private static void InheritFailure(JobNode node, JobNode dependency)
    {
        node.Errors.UnionWith(dependency.Errors);
    }
    
    // 激活任务,根据当前节点的情况决定下一步走向
    private void Activate(JobNode node)
    {
        if (node.Errors.Count != 0)
        {
            Finish(node, JobStatus.Skipped);
            return;
        }

        if (node.Length == 0)
        {
            Finish(node, JobStatus.Succeeded);
            return;
        }

        Enqueue(node);
    }

    private void Enqueue(JobNode node)
    {
        //更新当前任务状态为等待执行
        node.Status = JobStatus.Queued;
        int targetQueueIndex;
        
        //用于判断当前是否在工作线程上(调度线程上这个值为null)
        if (currentScheduler == this)
        {
            targetQueueIndex = workerIndex;
        }
        else
        {
            //调度线程分配,那就轮流给你调度到某条工作线程
            targetQueueIndex = nextQueue;
            nextQueue++;
            if (nextQueue == queues.Length)
                nextQueue = 0;
        }
            
        //一共要处理Length个任务,拆分成批次添加到待执行队列
        for (int begin = 0; begin < node.Length;)
        {
            //按一个批次数量来设定要取的范围
            int end = begin + Math.Min(node.BatchSize, node.Length - begin);
            //在对应工作线程的队列上添加该批次任务
            queues[targetQueueIndex].AddLast(new WorkItem(node, begin, end));
            //对于这个任务,更新它尚未结束的批次数
            node.Remaining++;
            //下一批从当前末尾继续
            begin = end;
        }

        //通知所有正在通过Monitor.Wait等待的线程,重新检查条件
        Monitor.PulseAll(gate);
    }
    
    //结束任务,并根据异常情况,指定最终状态
    private void Finish(JobNode initial, JobStatus status)
    {
        //创建一个队列,需要收尾的任务都会被放进来
        var pending = new Queue<(JobNode Node, JobStatus Status)>();
        pending.Enqueue((initial, status));
        //只要队列里还有内容,就说明还有待处理的任务
        while (pending.Count != 0)
        {
            //复制任务的最终状态
            var entry = pending.Dequeue();
            var node = entry.Node;
            node.Status = entry.Status;
            node.Body = null;
            //更新调度器中未完成的任务数
            unfinishedJobCount--;
            //对于当前任务,我们知道哪些任务依赖它
            //需要通知这些任务:该依赖项结束了,你不再需要等待它
            foreach (var child in node.Dependents)
            {
                //如果当前任务最终存在错误,那么也合并到等待它的节点错误中去
                InheritFailure(child, node);
                //更新等待者的所需依赖数
                if (--child.Dependencies != 0)
                {
                    continue;
                }
                //上面的条件通过说明等待者的所有依赖项已解决
                //已完成的依赖项存在错误=>等待者也进入结束队列(没有成功完成依赖项)
                //长度为0代表没有实际计算需求,是一个空任务,那么也进入结束队列
                if (child.Errors.Count != 0 || child.Length == 0)
                {
                    pending.Enqueue((child, child.Errors.Count != 0 ? JobStatus.Skipped : JobStatus.Succeeded));
                }
                //依赖项正常完成,父任务进入待开始队列
                else
                {
                    Enqueue(child);
                }
            }

            //该节点的所有父节点都知道它完成了,不再需要关联父节点
            node.Dependents.Clear();
        }

        //通知所有正在等待的线程,重新检查线程工作条件
        Monitor.PulseAll(gate);
    }
    
    private bool TryTake(int index, out WorkItem item)
    {
        //优先从自己的队尾领取任务(LIFO)
        var own = queues[index];
        if (own.Count != 0)
        {
            item = own.Last.Value;
            own.RemoveLast();
            return true;
        }
        
        //看看其他工作线程是否有可以做的任务
        for (int offset = 1; offset < queues.Length; offset++)
        {
            var other = queues[(index + offset) % queues.Length];
            if (other.Count == 0)
            {
                continue;
            }
                
            item = other.First.Value;
            other.RemoveFirst();
            return true;
        }

        item = default;
        return false;
    }

    private void WorkerLoop(int index)
    {
        //每个工作线程都有自己的专属编号
        currentScheduler = this;
        workerIndex = index;
        while (true)
        {
            WorkItem item;
            //加锁保证拿其他线程的工作时不会发生竞态问题
            //lock管理这一段代码的锁,进入时取得锁、离开时释放锁.
            //但中间可以通过Monitor.Wait暂时交出锁
            lock (gate)
            {
                while (!TryTake(index, out item))
                {
                    if (stopped)
                    {
                        return;
                    }
                        
                    //没找到可接取的任务,线程休眠等通知
                    Monitor.Wait(gate);
                }
            }

            Execute(item);
        }
    }

    private void Execute(WorkItem item)
    {
        lock (gate)
        {
            item.Node.Status = JobStatus.Running;
        }
            
        Exception error = null;
        try
        {
            for (int i = item.Begin; i < item.End; i++)
            {
                item.Node.Body(i);
            }
        }
        catch (Exception ex)
        {
            error = ex;
        }
        finally
        {
            lock (gate)
            {
                if (error != null)
                {
                    item.Node.Errors.Add(new JobFailure(error));
                }

                //更新剩余批数量,判断当前节点是否已全部完成
                item.Node.Remaining--;
                if (item.Node.Remaining == 0)
                {
                    Finish(item.Node, item.Node.Errors.Count == 0 ? JobStatus.Succeeded : JobStatus.Faulted);
                }
            }
        }
    }

    internal bool IsDone(JobNode node)
    {
        lock (gate)
        {
            return node.Done;
        }
    }

    internal JobStatus GetStatus(JobNode node)
    {
        lock (gate)
            return node.Status;
    }
    
    internal void Complete(JobNode node)
    {
        if (currentScheduler != null && currentScheduler != this)
        {
            throw new InvalidOperationException("Not your scheduer, worker.");
        }
        
        lock (gate)
        {
            while (!node.Done)
            {
                //我们应该禁止工作线程去执行这个方法:
                //假设:唯一的Worker正在运行JobA,JobA走到Completed,等待B(需要B完成才能完成整体任务)
                //但此时B没有Worker可以使用了,所以将会永远阻塞,永远无法完成
                //解决办法是显式提交Job的依赖,调度器会先让B完成,再去让A占用Worker
                if (currentScheduler == this)
                {
                    throw new InvalidOperationException("Workers cannot wait on unfinished jobs. Use job dependencies instead.");
                }
                    
                //虽然我们希望阻塞主线程直到工作线程完成
                //但我们仍然需要当前线程释放锁,因为工作线程也需要这把锁来更新任务状态
                Monitor.Wait(gate);
            }
            
            if (node.Errors.Count != 0)
            {
                var errors = new List<Exception>(node.Errors.Count);
                foreach (var failure in node.Errors)
                {
                    errors.Add(failure.Error);
                }
                throw new AggregateException(errors);
            }
        }
    }
}
