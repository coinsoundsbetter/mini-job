namespace MiniJob;

public enum JobStatus
{
    WaitingForDependencies, 
    Queued, 
    Running, 
    Succeeded, 
    Faulted, 
    Skipped
}
