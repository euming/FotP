using System;
using System.Linq;

namespace MemoryCtl;

internal static partial class Commands
{
    internal static (string Status, string Area, string Description) ParseRoadmapFields(string text, string fallbackTitle)
    {
        var status = "open";
        string? area = null;
        string? description = null;

        var lines = text.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);
        foreach (var raw in lines)
        {
            var line = raw.Trim();
            if (line.Length == 0) continue;

            if (line.StartsWith("Status:", StringComparison.OrdinalIgnoreCase))
            {
                var val = line.Substring("Status:".Length).Trim();
                if (!string.IsNullOrWhiteSpace(val))
                {
                    var lower = val.ToLowerInvariant();
                    if (lower.StartsWith("in_")) status = "in_progress";
                    else if (lower.StartsWith("in progress")) status = "in_progress";
                    else if (lower.StartsWith("done") || lower.StartsWith("complete")) status = "done";
                    else status = "open";
                }
                continue;
            }

            if (line.StartsWith("Area:", StringComparison.OrdinalIgnoreCase))
            {
                var val = line.Substring("Area:".Length).Trim();
                if (!string.IsNullOrWhiteSpace(val))
                    area = val;
                continue;
            }

            if (line.StartsWith("[status:", StringComparison.OrdinalIgnoreCase) && line.EndsWith("]"))
            {
                var inner = line.Substring(8, line.Length - 9).Trim();
                var lower = inner.ToLowerInvariant();
                if (lower == "open" || lower == "todo") status = "open";
                else if (lower is "in_progress" or "in-progress" or "doing") status = "in_progress";
                else if (lower is "done" or "complete" or "closed") status = "done";
                continue;
            }

            // First non-metadata line becomes the description.
            if (description == null)
                description = line;
        }

        if (string.IsNullOrWhiteSpace(description))
            description = fallbackTitle;

        return (status, area ?? string.Empty, description!);
    }

    public static string MakeSafeFileName(string memAnchorName)
    {
        if (string.IsNullOrWhiteSpace(memAnchorName))
            return "memAnchor";

        var chars = memAnchorName
            .Select(c => char.IsLetterOrDigit(c) ? c : '_')
            .ToArray();
        var s = new string(chars);

        while (s.Contains("__", StringComparison.Ordinal))
            s = s.Replace("__", "_", StringComparison.Ordinal);

        s = s.Trim('_');
        if (string.IsNullOrEmpty(s))
            s = "memAnchor";

        return s;
    }

    private static string OneLine(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return string.Empty;

        var s = text.Replace("\r\n", " ", StringComparison.Ordinal)
                    .Replace("\n", " ", StringComparison.Ordinal)
                    .Trim();
        while (s.Contains("  ", StringComparison.Ordinal))
            s = s.Replace("  ", " ", StringComparison.Ordinal);
        return s;
    }
}
