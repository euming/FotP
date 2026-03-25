using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using CardBinder.Core;

namespace MemoryCtl;

internal static class BinderRenderer
{
    public sealed record RenderOptions(
        string BinderName,
        int MaxChars,
        bool IncludeIds);

    public static string Render(string dbPath, RenderOptions opts)
    {
        var db = MemoryJsonlReader.Load(dbPath);

        // Resolve memAnchor id by exact (case-insensitive) match.
        MemAnchorId? memAnchor = null;
        foreach (var b in db.Core.AllBinders)
        {
            if (!db.Core.TryGetBinderName(b, out var n) || string.IsNullOrWhiteSpace(n)) continue;
            if (string.Equals(n, opts.BinderName, StringComparison.OrdinalIgnoreCase))
            {
                memAnchor = b;
                break;
            }
        }

        if (memAnchor == null)
            return "";

        var cards = db.Core.CardsIn(memAnchor.Value).ToList();
        if (cards.Count == 0)
            return "";

        var lines = new List<string>();
        lines.Add($"INJECT (memAnchor): {opts.BinderName}");
        lines.Add($"GeneratedAt: {DateTimeOffset.Now.ToString("o", CultureInfo.InvariantCulture)}");
        lines.Add("");

        foreach (var c in cards)
        {
            if (!db.TryGetPayload(c, out var payload) || payload == null)
                continue;

            var title = payload.Title ?? c.Value.ToString();
            var snippet = Snip(payload.Text ?? "", 240).Replace("\r\n", " ").Replace("\n", " ").Trim();

            var item = $"- {title}";
            if (opts.IncludeIds)
                item += $"  (id={c.Value})";
            lines.Add(item);

            if (!string.IsNullOrWhiteSpace(snippet))
                lines.Add($"  {snippet}");

            lines.Add("");

            if (lines.Sum(x => x.Length + 1) > opts.MaxChars)
                break;
        }

        return string.Join("\n", lines).TrimEnd();
    }

    private static string Snip(string s, int max)
    {
        if (string.IsNullOrWhiteSpace(s)) return "";
        var t = s.Trim();
        return t.Length <= max ? t : t.Substring(0, max) + "...";
    }
}
