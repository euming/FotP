using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using CardBinder.Core;

namespace MemoryCtl;

internal static class DeletedIndex
{
    private static readonly Regex LineRegex = new Regex("^\\[\\d{2}:\\d{2}\\]\\s+.+?:\\s+(.*)$", RegexOptions.Compiled);

    public sealed record DeletedSet(HashSet<Guid> CardIds, HashSet<Guid> MemAnchorIds);

    public static DeletedSet Load(string path)
    {
        if (!File.Exists(path)) return new DeletedSet(new HashSet<Guid>(), new HashSet<Guid>());

        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var root = doc.RootElement;
        var deleted = root.GetProperty("deleted");

        var cards = new HashSet<Guid>();
        if (deleted.TryGetProperty("cards", out var cardsArr) && cardsArr.ValueKind == JsonValueKind.Array)
        {
            foreach (var c in cardsArr.EnumerateArray())
                if (Guid.TryParse(c.GetString(), out var id)) cards.Add(id);
        }

        var memAnchors = new HashSet<Guid>();
        if (deleted.TryGetProperty("memAnchors", out var bindArr) && bindArr.ValueKind == JsonValueKind.Array)
        {
            foreach (var b in bindArr.EnumerateArray())
                if (Guid.TryParse(b.GetString(), out var id)) memAnchors.Add(id);
        }

        return new DeletedSet(cards, memAnchors);
    }

    public static HashSet<string> BuildExcludedTextSet(DeletedSet deleted, MemoryDb db)
    {
        var set = new HashSet<string>(StringComparer.Ordinal);
        foreach (var id in deleted.CardIds)
        {
            var cardId = new CardId(id);
            if (!db.TryGetPayload(cardId, out var payload) || payload == null) continue;
            if (string.IsNullOrWhiteSpace(payload.Text)) continue;

            var lines = payload.Text.Split('\n');
            foreach (var line in lines)
            {
                var m = LineRegex.Match(line);
                if (!m.Success) continue;
                var msg = m.Groups[1].Value.Trim();
                if (string.IsNullOrWhiteSpace(msg)) continue;
                var norm = TranscriptBuilder.NormalizeForMatch(msg);
                if (!string.IsNullOrWhiteSpace(norm))
                    set.Add(norm);
            }
        }

        return set;
    }
}
