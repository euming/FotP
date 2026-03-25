using AMS.Core;

namespace MemoryCtl;

internal sealed partial class GraphCommandModule
{
    public int RelayRun(string dbPath, string taskThreadId, string? injectHandoffFailureMode)
    {
        var store = _runtimeFactory.LoadAmsStore(dbPath);
        if (store is null)
        {
            Console.Error.WriteLine("error: relay-run requires the AMS backend.");
            return 1;
        }

        var result = new RelayRunService(store).Run(taskThreadId, injectHandoffFailureMode, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);

        Console.WriteLine($"task_thread_id={result.TaskThreadId}");
        Console.WriteLine($"run_artifact_ref={result.RunArtifactRef}");
        Console.WriteLine($"verdict={result.Verdict}");
        Console.WriteLine($"stop_reason={result.StopReason}");
        Console.WriteLine($"repair_thread_id={result.RepairThreadId ?? "none"}");
        Console.WriteLine($"archive_refs={(result.ArchiveRefs.Count == 0 ? "none" : string.Join(", ", result.ArchiveRefs))}");
        return 0;
    }
}
