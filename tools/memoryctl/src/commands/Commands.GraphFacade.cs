using AMS.Core;
using System;
using System.Collections.Generic;

namespace MemoryCtl;

internal static partial class Commands
{
    private static readonly GraphCommandModule DefaultGraphCommands = new(new LegacyCommandRuntimeFactory());

    public static int Add(string dbPath, string title, string text, IReadOnlyList<string> memAnchors, string? source, string? key)
        => DefaultGraphCommands.Add(dbPath, title, text, memAnchors, source, key);

    public static int Maintain(string dbPath, Guid cardId, int top, bool apply, string reason, float relevance)
        => DefaultGraphCommands.Maintain(dbPath, cardId, top, apply, reason, relevance);

    public static int Query(string dbPath, string query, int top, string? binderFilter, bool explain)
        => DefaultGraphCommands.Query(dbPath, query, top, binderFilter, explain);

    public static int MakeBinder(string dbPath, string binderName, string query, int top, string? binderFilter, float relevance, string reason)
        => DefaultGraphCommands.MakeMemAnchor(dbPath, binderName, query, top, binderFilter, relevance, reason);

    public static int Prompt(string dbPath, string query, int top, IReadOnlyList<string> binderFilters)
        => DefaultGraphCommands.Prompt(dbPath, query, top, binderFilters, Array.Empty<string>(), Array.Empty<string>(), top, ContextObjectOrdering.Ordinal);
}
