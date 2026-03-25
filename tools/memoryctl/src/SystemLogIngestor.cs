using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MemoryGraph.Abstractions;

namespace MemoryCtl;

internal static class SystemLogIngestor
{
    public sealed record IngestResult(int FilesSeen, int EntriesRead, int EntriesNew, int CardsCreated);

    public static IngestResult Ingest(
        string dbPath,
        string logDir,
        string cursorDir,
        int maxEntries,
        ICommandRuntimeFactory runtimeFactory)
    {
        ArgumentNullException.ThrowIfNull(runtimeFactory);

        MemoryJsonlWriter.EnsureHeaderExists(dbPath);

        Directory.CreateDirectory(cursorDir);

        var files = Directory.Exists(logDir)
            ? Directory.GetFiles(logDir, "*.log", SearchOption.TopDirectoryOnly)
            : Array.Empty<string>();

        int filesSeen = 0;
        int entriesRead = 0;
        int entriesNew = 0;
        int cardsCreated = 0;

        // Load runtime once for backend-agnostic ingest operations.
        var runtime = runtimeFactory.Load(dbPath);
        var binderCache = MemoryJsonlWriter.CreateBinderCache(runtime.Db);

        foreach (var file in files.OrderBy(f => f, StringComparer.OrdinalIgnoreCase))
        {
            filesSeen++;
            var fileName = Path.GetFileName(file);
            var jobName = Path.GetFileNameWithoutExtension(fileName);

            var cursorPath = Path.Combine(cursorDir, fileName + ".cursor.json");
            var cursor = CursorStore.Load(cursorPath);

            foreach (var (endLine, entry) in SystemLogReader.ReadEntries(file, cursor.LastLineNumber, jobName))
            {
                entriesRead++;
                entriesNew++;

                // One card per entry (timestamp block). Stable id via line range.
                var key = $"systemlog:{fileName}:lines:{cursor.LastLineNumber + 1}-{endLine}";
                var cardId = GuidUtil.FromKey("card:" + key);

                var title = $"System log: {jobName} {entry.Ts:yyyy-MM-dd HH:mm:ss}";

                var binderNames = new List<string>
                {
                    "System Logs",
                    $"Job: {jobName}",
                    entry.IsError ? "System Logs: Errors" : (entry.IsWarning ? "System Logs: Warnings" : "System Logs: Routine")
                };

                var resolved = MemoryJsonlWriter.ResolveBinders(binderCache, binderNames);
                var now = DateTimeOffset.Now;
                var eventTimestamp = entry.Ts;
                var source = $"systemlog:{fileName}";
                var relevance = entry.IsError ? 0.95f : 0.5f;

                runtime.IngestService.UpsertCard(cardId, runtime.Payloads, title, entry.Text, source, eventTimestamp);
                if (runtime.GraphStore is MemoryGraph.Infrastructure.AMS.AmsGraphStoreAdapter amsGraphStore)
                {
                    amsGraphStore.AttachCardToTime(cardId, eventTimestamp);
                }
                foreach (var (memAnchorId, memAnchorName) in resolved)
                {
                    runtime.IngestService.UpsertMemAnchor(memAnchorId, memAnchorName);
                    runtime.IngestService.LinkCardToMemAnchor(cardId, memAnchorId, new MemoryLinkMeta(
                        Relevance: relevance,
                        Reason: "ingest-systemlogs",
                        CreatedAt: now));
                }
                runtimeFactory.SyncPayloadsToDb(runtime);

                // Preserve append-only JSONL persistence behavior/output format.
                MemoryJsonlWriter.AppendCard(
                    dbPath: dbPath,
                    cardId: cardId,
                    title: title,
                    text: entry.Text,
                    source: source,
                    now: now,
                    binders: resolved,
                    relevance: relevance,
                    reason: "ingest-systemlogs");

                cardsCreated++;

                // advance cursor progressively
                cursor = new ChatCursor(endLine, entry.Ts, null);
                CursorStore.Save(cursorPath, cursor);

                if (entriesNew >= maxEntries)
                    break;
            }

            if (entriesNew >= maxEntries)
                break;
        }

        return new IngestResult(filesSeen, entriesRead, entriesNew, cardsCreated);
    }
}
