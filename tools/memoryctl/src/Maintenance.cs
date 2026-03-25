using System;
using System.Collections.Generic;
using System.Linq;
using CardBinder.Core;

namespace MemoryCtl;

internal static class Maintenance
{
    public sealed record RelatedCard(CardId CardId, string Title, double Score, IReadOnlyList<string> Tokens);

    public static IReadOnlyList<RelatedCard> FindRelatedCards(MemoryDb db, CardId seedCard, int top)
    {
        if (!db.TryGetPayload(seedCard, out var seedPayload))
            return Array.Empty<RelatedCard>();

        var seedText = ((seedPayload.Title ?? "") + "\n" + (seedPayload.Text ?? "")).Trim();
        var seedTokens = Scoring.Tokenize(seedText);
        var seedSet = new HashSet<string>(seedTokens);
        if (seedSet.Count == 0) return Array.Empty<RelatedCard>();

        var related = new List<RelatedCard>();
        foreach (var c in db.Core.AllCards)
        {
            if (c.Equals(seedCard)) continue;
            if (db.Core.GetState(c) == CardState.Retracted) continue;
            if (!db.TryGetPayload(c, out var payload)) continue;

            var text = ((payload.Title ?? "") + "\n" + (payload.Text ?? "")).Trim();
            var tokens = Scoring.Tokenize(text);
            if (tokens.Count == 0) continue;

            var overlap = tokens.Count(t => seedSet.Contains(t));
            if (overlap == 0) continue;

            // Simple weighted score: overlap + small boost for meta relevance.
            double metaBoost = 0;
            foreach (var b in db.Core.BindersOf(c))
            {
                if (db.Core.TryGetLinkMeta(c, b, out var meta))
                    metaBoost += Math.Clamp(meta.Relevance, 0, 1) * 0.1;
            }

            var score = overlap + metaBoost;
            related.Add(new RelatedCard(c, payload.Title ?? c.Value.ToString(), score, tokens));
        }

        return related
            .OrderByDescending(r => r.Score)
            .Take(top)
            .ToList();
    }

    public static IReadOnlyList<(string binderName, double score)> SuggestBindersFromRelated(MemoryDb db, IEnumerable<RelatedCard> related, int top)
    {
        Func<MemAnchorId, string?> binderName = b => db.Core.TryGetBinderName(b, out var n) ? n : null;

        var scores = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        foreach (var r in related)
        {
            foreach (var b in db.Core.BindersOf(r.CardId))
            {
                var name = binderName(b);
                if (string.IsNullOrWhiteSpace(name)) continue;

                // Weight by related-card score; lightly damp so one card doesn't dominate.
                var w = Math.Min(3.0, r.Score) * 0.5;

                scores.TryGetValue(name!, out var cur);
                scores[name!] = cur + w;
            }
        }

        return scores
            .Select(kvp => (binderName: kvp.Key, score: kvp.Value))
            .OrderByDescending(x => x.score)
            .Take(top)
            .ToList();
    }
}
