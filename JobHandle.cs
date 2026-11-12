using System;

namespace MiniJob;

public readonly struct JobHandle
{
    internal readonly JobNode Node;
    internal JobHandle(JobNode node) => Node = node;
    public bool IsCompleted => Node == null || Node.Owner.IsDone(Node);
    public JobStatus Status => Node == null ? JobStatus.Succeeded : Node.Owner.GetStatus(Node);
    public void Complete()
    {
        if (Node != null) {
            Node.Owner.Complete(Node);
        }
    }
    
    public static JobHandle CombineDependencies(JobHandle first, JobHandle second) =>
        CombineDependencies(new JobHandle[] { first, second });
    
    public static JobHandle CombineDependencies(ReadOnlySpan<JobHandle> handles)
    {
        JobScheduler owner = null;
        foreach (var handle in handles)
        {
            if (handle.Node == null) {
                continue;
            }

            if (owner != null && owner != handle.Node.Owner) {
                throw new ArgumentException("Dependencies must belong to the same scheduler.");
            }
                
            owner = handle.Node.Owner;
        }
        
        return owner?.Combine(handles) ?? default;
    }
}
