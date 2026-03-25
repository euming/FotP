using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CardBinder.Core;

namespace MemoryCtl;

internal static partial class Commands
{
    public static int Validate(string dbPath)
    {
        _ = MemoryJsonlReader.Load(dbPath);
        Console.WriteLine("OK");
        return 0;
    }

    public static int ExportGraph(string dbPath, string outPath)
    {
        var db = MemoryJsonlReader.Load(dbPath);

        Func<MemAnchorId, string?> binderName = b => db.Core.TryGetBinderName(b, out var n) ? n : null;

        var nodes = new List<object>();
        var edges = new List<object>();

        // memAnchors (binders)
        // Viewer-level de-duplication by memAnchor name so the graph UI doesn't show
        // multiple tiles for the same logical memAnchor. Underlying DB / JSONL remain
        // unchanged; this only affects the exported view.
        var seenMemAnchorNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var b in db.Core.AllBinders)
        {
            var name = binderName(b);
            if (string.IsNullOrWhiteSpace(name))
                continue;

            var normalized = name.Trim();
            if (string.IsNullOrEmpty(normalized))
                continue;

            // Skip true name-duplicates; keep the first occurrence.
            if (!seenMemAnchorNames.Add(normalized))
                continue;

            nodes.Add(new
            {
                id = b.Value,
                kind = "memAnchor",
                label = name,
                memAnchorName = name,
                safeFileName = MakeSafeFileName(name!)
            });
        }

        // cards
        foreach (var c in db.Core.AllCards)
        {
            db.TryGetPayload(c, out var payload);
            var title = payload?.Title ?? c.Value.ToString();

            nodes.Add(new
            {
                id = c.Value,
                kind = "card",
                label = title,
                memAnchorName = (string?)null,
                safeFileName = (string?)null
            });
        }

        // TagLinks: memAnchor -> card
        foreach (var b in db.Core.AllBinders)
        {
            foreach (var c in db.Core.CardsIn(b))
            {
                edges.Add(new
                {
                    from = b.Value,
                    to = c.Value,
                    kind = "taglink"
                });
            }
        }

        var graph = new
        {
            nodes,
            edges
        };

        var json = System.Text.Json.JsonSerializer.Serialize(graph, new System.Text.Json.JsonSerializerOptions
        {
            WriteIndented = true
        });

        var outDir = Path.GetDirectoryName(outPath);
        if (!string.IsNullOrWhiteSpace(outDir) && !Directory.Exists(outDir))
            Directory.CreateDirectory(outDir);

        File.WriteAllText(outPath, json);
        Console.WriteLine(outPath);
        return 0;
    }

    public static int SuggestBinders(string dbPath, string query, int top)
    {
        var db = MemoryJsonlReader.Load(dbPath);

        var tokens = Scoring.Tokenize(query);
        Func<MemAnchorId, string?> binderName = b => db.Core.TryGetBinderName(b, out var n) ? n : null;

        var scored = new List<(string name, double score)>();
        foreach (var b in db.Core.AllBinders)
        {
            var name = binderName(b);
            if (string.IsNullOrWhiteSpace(name)) continue;
            var nl = name.ToLowerInvariant();
            double s = 0;
            foreach (var t in tokens)
            {
                if (nl.Contains(t)) s += 1.0;
            }
            if (s > 0)
                scored.Add((name!, s));
        }

        foreach (var (name, score) in scored.OrderByDescending(x => x.score).Take(top))
            Console.WriteLine($"- {name}  (score={score:0.##})");

        return 0;
    }

    public static int ListMemAnchors(string dbPath)
    {
        var db = MemoryJsonlReader.Load(dbPath);

        Func<MemAnchorId, string?> binderName = b => db.Core.TryGetBinderName(b, out var n) ? n : null;

        var names = db.Core.AllBinders
            .Select(b => binderName(b))
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase);

        foreach (var name in names)
            Console.WriteLine(name);

        return 0;
    }
}
