namespace MemoryCtl;

internal sealed class IngestSyncCommandModule
{
    private readonly ICommandRuntimeFactory _runtimeFactory;

    public IngestSyncCommandModule(ICommandRuntimeFactory runtimeFactory)
    {
        _runtimeFactory = runtimeFactory ?? throw new ArgumentNullException(nameof(runtimeFactory));
    }

    public int IngestChatlog(string dbPath, string chatlogPath, string cursorPath, int max, int gapMinutes, bool dream, string? rawLlmPath = null)
    {
        // Ensure destination exists / is well-formed before ingest.
        MemoryJsonlWriter.EnsureHeaderExists(dbPath);

        var result = ChatIngestor.Ingest(
            dbPath: dbPath,
            chatlogPath: chatlogPath,
            cursorPath: cursorPath,
            maxEvents: max,
            gapMinutes: gapMinutes,
            applyMaintenance: dream,
            runtimeFactory: _runtimeFactory,
            rawLlmPath: rawLlmPath);

        var suffix = result.LinesMalformed > 0 ? $" Malformed={result.LinesMalformed}" : "";
        Console.WriteLine($"Read={result.EventsRead} New={result.EventsIngested} Cards={result.CreatedCardIds.Count}{suffix}");
        foreach (var id in result.CreatedCardIds)
            Console.WriteLine(id);

        return 0;
    }

    public int IngestSystemLogs(string dbPath, string logDir, string cursorDir, int max)
    {
        var result = SystemLogIngestor.Ingest(dbPath, logDir, cursorDir, maxEntries: max, runtimeFactory: _runtimeFactory);
        Console.WriteLine($"Files={result.FilesSeen} Read={result.EntriesRead} New={result.EntriesNew} Cards={result.CardsCreated}");
        return 0;
    }

    public int SyncRawLlmFromSessions(string sessionsJson, string rawLlmDir, string cursorDir)
    {
        var result = SessionLlmSync.SyncTelegramSessionsToRawLlm(
            sessionsJsonPath: sessionsJson,
            rawLlmDir: rawLlmDir,
            cursorDir: cursorDir,
            skipNoReply: true);

        Console.WriteLine($"Sessions={result.SessionsSeen} AssistantMsgs={result.AssistantMessagesSeen} Appended={result.Appended} Dupes={result.SkippedDupes}");
        return 0;
    }

    public int SyncRawUserFromSessions(string sessionsJson, string rawUserDir, string cursorDir)
    {
        var result = SessionUserSync.SyncTelegramSessionsToRawUser(
            sessionsJsonPath: sessionsJson,
            rawUserDir: rawUserDir,
            cursorDir: cursorDir);

        Console.WriteLine($"Sessions={result.SessionsSeen} UserMsgs={result.UserMessagesSeen} Appended={result.Appended} Dupes={result.SkippedDupes}");
        return 0;
    }
}
