using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using CardBinder.Core;

namespace MemoryCtl;

internal static class MemoryJsonlWriter
{
    public static void AppendLinks(
        string dbPath,
        Guid cardId,
        DateTimeOffset now,
        IReadOnlyList<(Guid binderId, string binderName)> binders,
        float relevance,
        string reason,
        string addedBy = "memoryctl")
    {
        EnsureHeaderExists(dbPath);

        using var sw = File.AppendText(dbPath);

        foreach (var b in binders)
            sw.WriteLine(JsonSerializer.Serialize(new { type = "binder", id = b.binderId, name = b.binderName }));

        foreach (var b in binders)
        {
            sw.WriteLine(JsonSerializer.Serialize(new
            {
                type = "taglink",
                card_id = cardId,
                binder_id = b.binderId,
                meta = new { Relevance = relevance, Reason = reason, AddedBy = addedBy, CreatedAt = now.ToString("o") }
            }));
        }
    }

    public static void EnsureHeaderExists(string dbPath)
    {
        if (!File.Exists(dbPath))
        {
            Directory.CreateDirectory(Path.GetDirectoryName(dbPath) ?? ".");
            File.WriteAllText(dbPath, "{\"type\":\"format\",\"name\":\"card-binder\",\"version\":1}" + Environment.NewLine);
            return;
        }

        // Validate first non-empty line is the header.
        using var fs = File.OpenRead(dbPath);
        using var sr = new StreamReader(fs);
        string? line;
        while ((line = sr.ReadLine()) != null)
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;
            if (root.TryGetProperty("type", out var t) && t.GetString() == "format")
                return;
            throw new FormatException("memory.jsonl missing format header as first record.");
        }

        throw new FormatException("memory.jsonl is empty; missing format header.");
    }

    public static void AppendCard(
        string dbPath,
        Guid cardId,
        string title,
        string text,
        string? source,
        DateTimeOffset now,
        IReadOnlyList<(Guid binderId, string binderName)> binders,
        float relevance = 0.9f,
        string reason = "on-demand add")
    {
        EnsureHeaderExists(dbPath);

        using var sw = File.AppendText(dbPath);

        foreach (var b in binders)
        {
            sw.WriteLine(JsonSerializer.Serialize(new { type = "binder", id = b.binderId, name = b.binderName }));
        }

        sw.WriteLine(JsonSerializer.Serialize(new { type = "card", id = cardId, state = "Active" }));

        foreach (var b in binders)
        {
            sw.WriteLine(JsonSerializer.Serialize(new
            {
                type = "taglink",
                card_id = cardId,
                binder_id = b.binderId,
                meta = new { Relevance = relevance, Reason = reason, AddedBy = "memoryctl", CreatedAt = now.ToString("o") }
            }));
        }

        sw.WriteLine(JsonSerializer.Serialize(new
        {
            type = "card_payload",
            card_id = cardId,
            title,
            text,
            source,
            updated_at = now.ToString("o")
        }));
    }

    public static Dictionary<string, Guid> CreateBinderCache(MemoryDb db)
    {
        var binderNameLookup = new Dictionary<string, Guid>(StringComparer.OrdinalIgnoreCase);
        foreach (var b in db.Core.AllBinders)
        {
            if (!db.Core.TryGetBinderName(b, out var n) || string.IsNullOrWhiteSpace(n)) continue;
            // If duplicates exist in the file, keep the first one we saw.
            if (!binderNameLookup.ContainsKey(n))
                binderNameLookup[n] = b.Value;
        }
        return binderNameLookup;
    }

    public static IReadOnlyList<(Guid binderId, string binderName)> ResolveBinders(Dictionary<string, Guid> cache, IEnumerable<string> binderNames)
    {
        var list = new List<(Guid, string)>();
        foreach (var name in binderNames.Select(n => n.Trim()).Where(n => !string.IsNullOrWhiteSpace(n)))
        {
            if (cache.TryGetValue(name, out var id))
            {
                list.Add((id, name));
            }
            else
            {
                var newId = Guid.NewGuid();
                cache[name] = newId;
                list.Add((newId, name));
            }
        }
        return list;
    }

    public static IReadOnlyList<(Guid binderId, string binderName)> ResolveBinders(MemoryDb db, IEnumerable<string> binderNames)
    {
        var cache = CreateBinderCache(db);
        return ResolveBinders(cache, binderNames);
    }
}
