using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using CardBinder.Core;

namespace MemoryCtl;

public static class Scoring
{
    private static readonly Regex TokenRx = new(@"[A-Za-z0-9_]+", RegexOptions.Compiled);

    public sealed record ScoreDetail(double Total, double TextScore, double BinderScore, double MetaScore);

    public static IReadOnlyList<string> Tokenize(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return Array.Empty<string>();
        return TokenRx.Matches(s)
            .Select(m => m.Value.ToLowerInvariant())
            .Where(t => t.Length >= 2)
            .Distinct()
            .ToList();
    }

    public static ScoreDetail ScoreCard(
        string query,
        CardId card,
        MemoryDb db,
        Func<MemAnchorId, string?> binderNameLookup)
    {
        var tokens = Tokenize(query);

        double textScore = 0;
        if (db.TryGetPayload(card, out var payload))
        {
            string hay = ((payload.Title ?? "") + "\n" + (payload.Text ?? "")).ToLowerInvariant();
            foreach (var tok in tokens)
            {
                if (hay.Contains(tok)) textScore += 1.0;
            }
        }

        // MemAnchor match (names)
        double binderScore = 0;
        foreach (var b in db.Core.BindersOf(card))
        {
            var nm = binderNameLookup(b);
            if (string.IsNullOrEmpty(nm)) continue;
            var nml = nm.ToLowerInvariant();
            foreach (var tok in tokens)
            {
                if (nml.Contains(tok)) binderScore += 0.25;
            }
        }

        // Meta score: sum relevance across memAnchors (bounded)
        double metaScore = 0;
        foreach (var b in db.Core.BindersOf(card))
        {
            if (db.Core.TryGetLinkMeta(card, b, out var meta))
                metaScore += Math.Clamp(meta.Relevance, 0, 1) * 0.5;
        }

        double total = textScore + binderScore + metaScore;
        return new ScoreDetail(total, textScore, binderScore, metaScore);
    }
}
