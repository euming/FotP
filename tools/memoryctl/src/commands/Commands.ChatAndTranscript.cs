using System;

namespace MemoryCtl;

internal static partial class Commands
{
    public static int AppendChatEvent(string chatlogPath, string channel, string chatId, string messageId, DateTimeOffset ts, string? author, string direction, string text)
    {
        var evt = new ChatEvent(
            Channel: channel,
            ChatId: chatId,
            MessageId: messageId,
            Ts: ts,
            Author: author,
            Direction: direction,
            Text: text);

        var appended = ChatLogWriter.AppendChatEvent(chatlogPath, evt);
        Console.WriteLine(appended ? "APPENDED" : "SKIPPED_DUPLICATE");
        return 0;
    }

    public static int IngestChatlog(string dbPath, string chatlogPath, string cursorPath, int max, int gapMinutes, bool dream)
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
            runtimeFactory: new LegacyCommandRuntimeFactory());

        var suffix = result.LinesMalformed > 0 ? $" Malformed={result.LinesMalformed}" : "";
        Console.WriteLine($"Read={result.EventsRead} New={result.EventsIngested} Cards={result.CreatedCardIds.Count}{suffix}");
        foreach (var id in result.CreatedCardIds)
            Console.WriteLine(id);

        return 0;
    }

    public static int BuildTranscript(string rawUserPath, string rawLlmPath, string outJsonlPath, string? outMdPath, string? outHtmlPath, string channel, string chatId)
    {
        TranscriptBuilder.Build(
            rawUserPath: rawUserPath,
            rawLlmPath: rawLlmPath,
            outJsonlPath: outJsonlPath,
            outMdPath: outMdPath,
            outHtmlPath: outHtmlPath,
            opts: new TranscriptBuilder.Options(
                Channel: channel,
                ChatId: chatId,
                IncludeAssistant: true,
                IncludeUser: true,
                IncludeRawUserOutbound: false));

        Console.WriteLine(outJsonlPath);
        if (!string.IsNullOrWhiteSpace(outMdPath))
            Console.WriteLine(outMdPath);
        if (!string.IsNullOrWhiteSpace(outHtmlPath))
            Console.WriteLine(outHtmlPath);

        return 0;
    }

    public static int BuildTranscriptClean(
        string rawUserPath,
        string rawLlmPath,
        string outJsonlPath,
        string? outMdPath,
        string? outHtmlPath,
        string channel,
        string chatId,
        string dbPath,
        string deletedPath)
    {
        var deleted = DeletedIndex.Load(deletedPath);
        var db = MemoryJsonlReader.Load(dbPath);
        var exclude = DeletedIndex.BuildExcludedTextSet(deleted, db);

        TranscriptBuilder.Build(
            rawUserPath: rawUserPath,
            rawLlmPath: rawLlmPath,
            outJsonlPath: outJsonlPath,
            outMdPath: outMdPath,
            outHtmlPath: outHtmlPath,
            opts: new TranscriptBuilder.Options(
                Channel: channel,
                ChatId: chatId,
                IncludeAssistant: true,
                IncludeUser: true,
                IncludeRawUserOutbound: false),
            excludeNormalizedText: exclude);

        Console.WriteLine(outJsonlPath);
        if (!string.IsNullOrWhiteSpace(outMdPath))
            Console.WriteLine(outMdPath);
        if (!string.IsNullOrWhiteSpace(outHtmlPath))
            Console.WriteLine(outHtmlPath);

        return 0;
    }

    public static int SyncRawLlmFromSessions(string sessionsJson, string rawLlmDir, string cursorDir)
    {
        var r = SessionLlmSync.SyncTelegramSessionsToRawLlm(
            sessionsJsonPath: sessionsJson,
            rawLlmDir: rawLlmDir,
            cursorDir: cursorDir,
            skipNoReply: true);

        Console.WriteLine($"Sessions={r.SessionsSeen} AssistantMsgs={r.AssistantMessagesSeen} Appended={r.Appended} Dupes={r.SkippedDupes}");
        return 0;
    }

    public static int SyncRawUserFromSessions(string sessionsJson, string rawUserDir, string cursorDir)
    {
        var r = SessionUserSync.SyncTelegramSessionsToRawUser(
            sessionsJsonPath: sessionsJson,
            rawUserDir: rawUserDir,
            cursorDir: cursorDir);

        Console.WriteLine($"Sessions={r.SessionsSeen} UserMsgs={r.UserMessagesSeen} Appended={r.Appended} Dupes={r.SkippedDupes}");
        return 0;
    }

    public static int IngestSystemLogs(string dbPath, string logDir, string cursorDir, int max)
    {
        var r = SystemLogIngestor.Ingest(dbPath, logDir, cursorDir, maxEntries: max, runtimeFactory: new LegacyCommandRuntimeFactory());
        Console.WriteLine($"Files={r.FilesSeen} Read={r.EntriesRead} New={r.EntriesNew} Cards={r.CardsCreated}");
        return 0;
    }
}
