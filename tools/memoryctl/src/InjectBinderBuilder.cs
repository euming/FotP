using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using CardBinder.Core;

namespace MemoryCtl;

internal static class InjectBinderBuilder
{
    public sealed record BuildOptions(
        string Channel,
        string ChatId,
        string ChatLabel,
        string Query,
        int Top,
        int MaxLinks,
        float Relevance,
        string Reason,
        bool PerRunBinder);

    public static string Build(
        string dbPath,
        DeltaContext.Options deltaOpts,
        BuildOptions opts)
    {
        var db = MemoryJsonlReader.Load(dbPath);

        // Select cards the same way delta does (overlay memAnchors, optionally chat-scoped).
        var selected = DeltaContext.SelectCards(db, deltaOpts);
        if (selected.Count == 0)
            return "";

        var now = DateTimeOffset.Now;
        var binderName = opts.PerRunBinder
            ? $"Inject: {opts.ChatLabel} @ {now:yyyy-MM-dd HH:mm:ss}"
            : $"Inject: {opts.ChatLabel}";

        // Resolve memAnchor id (create if missing)
        var resolved = MemoryJsonlWriter.ResolveBinders(db, new[] { binderName });

        // Link top N
        int linked = 0;
        foreach (var c in selected.Take(Math.Max(1, opts.MaxLinks)))
        {
            MemoryJsonlWriter.AppendLinks(
                dbPath: dbPath,
                cardId: c.Value,
                now: now,
                binders: resolved,
                relevance: opts.Relevance,
                reason: opts.Reason,
                addedBy: "inject-memAnchor");

            linked++;
            if (linked >= opts.MaxLinks) break;
        }

        return binderName;
    }
}
