using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;

namespace MemoryCtl;

internal static class TranscriptBuilder
{
    public sealed record Options(
        string Channel,
        string ChatId,
        bool IncludeAssistant,
        bool IncludeUser,
        bool IncludeRawUserOutbound);

    private sealed record Line(DateTimeOffset Ts, string Role, string Speaker, string Text, string SourceKind, string SourceId, string? ParentId);

    public static void Build(
        string rawUserPath,
        string rawLlmPath,
        string outJsonlPath,
        string? outMdPath,
        string? outHtmlPath,
        Options opts,
        HashSet<string>? excludeNormalizedText = null)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(outJsonlPath) ?? ".");

        var lines = new List<Line>();

        if (opts.IncludeUser && File.Exists(rawUserPath))
        {
            foreach (var (_, evt) in ChatLog.ReadEvents(rawUserPath, onMalformedLine: (_, __, ___) => { }))
            {
                if (!string.Equals(evt.Channel, opts.Channel, StringComparison.OrdinalIgnoreCase)) continue;
                if (!string.Equals(evt.ChatId, opts.ChatId, StringComparison.Ordinal)) continue;

                // rawUser is meant to be inbound-only. However, if outbound leaked into it,
                // we can drop it by default to avoid duplicates (assistant completions come from rawLLM).
                if (!opts.IncludeRawUserOutbound && string.Equals(evt.Direction, "out", StringComparison.OrdinalIgnoreCase))
                    continue;

                var role = string.Equals(evt.Direction, "out", StringComparison.OrdinalIgnoreCase) ? "assistant" : "user";
                if (role == "assistant" && !opts.IncludeAssistant) continue;

                if (LooksLikeAutomationSpeaker(evt.Author))
                    continue;

                var speaker = NormalizeSpeaker(evt.Author, role);
                if (excludeNormalizedText == null || !excludeNormalizedText.Contains(NormalizeForMatch(evt.Text)))
                    lines.Add(new Line(evt.Ts, role, speaker, evt.Text, "rawUser", evt.MessageId, null));
            }
        }

        if (opts.IncludeAssistant && File.Exists(rawLlmPath))
        {
            foreach (var (_, c) in RawLlmLog.ReadCompletions(rawLlmPath, onMalformedLine: (_, __, ___) => { }))
            {
                if (!string.Equals(c.Channel, opts.Channel, StringComparison.OrdinalIgnoreCase)) continue;
                if (!string.Equals(c.ChatId, opts.ChatId, StringComparison.Ordinal)) continue;

                // Drop automation spam (cron/job summaries) from user-facing transcripts.
                if (LooksLikeAutomation(c.Text))
                    continue;

                if (excludeNormalizedText == null || !excludeNormalizedText.Contains(NormalizeForMatch(c.Text)))
                    lines.Add(new Line(c.Ts, "assistant", "Rocky", c.Text, "rawLLM", c.CompletionMessageId, c.ParentMessageId));
            }
        }

        // Stable ordering: ts, then role (user first), then source id.
        var ordered = lines
            .OrderBy(l => l.Ts)
            .ThenBy(l => l.Role == "user" ? 0 : 1)
            .ThenBy(l => l.SourceId, StringComparer.Ordinal)
            .ToList();

        using (var sw = new StreamWriter(File.Open(outJsonlPath, FileMode.Create, FileAccess.Write, FileShare.Read), System.Text.Encoding.UTF8))
        {
            foreach (var l in ordered)
            {
                var obj = new Dictionary<string, object?>
                {
                    ["type"] = "transcript_line",
                    ["channel"] = opts.Channel,
                    ["chat_id"] = opts.ChatId,
                    ["role"] = l.Role,
                    ["ts"] = l.Ts.ToString("o", CultureInfo.InvariantCulture),
                    ["speaker"] = l.Speaker,
                    ["text"] = l.Text,
                    ["ref"] = new Dictionary<string, object?>
                    {
                        ["source"] = l.SourceKind,
                        ["id"] = l.SourceId,
                        ["parent_id"] = l.ParentId
                    }
                };

                sw.WriteLine(JsonSerializer.Serialize(obj));
            }
        }

        if (!string.IsNullOrWhiteSpace(outMdPath))
        {
            Directory.CreateDirectory(Path.GetDirectoryName(outMdPath) ?? ".");

            using var md = new StreamWriter(File.Open(outMdPath, FileMode.Create, FileAccess.Write, FileShare.Read), System.Text.Encoding.UTF8);
            md.WriteLine($"# Transcript: {opts.Channel}:{opts.ChatId}");
            md.WriteLine();

            foreach (var l in ordered)
            {
                var text = NormalizeForDisplay(l.Text);
                var who = string.IsNullOrWhiteSpace(l.Speaker) ? (l.Role == "user" ? "User" : "Assistant") : l.Speaker;

                // Chat-app-ish style: header line + body, separated by blank line.
                md.WriteLine($"{who}  [{l.Ts:yyyy-MM-dd HH:mm}]\n{text}");
                md.WriteLine();
            }
        }

        if (!string.IsNullOrWhiteSpace(outHtmlPath))
        {
            Directory.CreateDirectory(Path.GetDirectoryName(outHtmlPath) ?? ".");

            using var html = new StreamWriter(File.Open(outHtmlPath, FileMode.Create, FileAccess.Write, FileShare.Read), System.Text.Encoding.UTF8);
            html.WriteLine("<!doctype html>");
            html.WriteLine("<html lang=\"en\">");
            html.WriteLine("<head>");
            html.WriteLine("  <meta charset=\"utf-8\" />");
            html.WriteLine($"  <title>Transcript: {WebUtility.HtmlEncode(opts.Channel)}:{WebUtility.HtmlEncode(opts.ChatId)}</title>");
            html.WriteLine("  <style>");
            html.WriteLine("    :root { color-scheme: light dark; }");
            html.WriteLine("    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 24px; }\n    h1 { font-size: 18px; margin-bottom: 16px; }\n    .msg { padding: 10px 12px; border-radius: 10px; margin: 10px 0; border: 1px solid #ccc3; }\n    .msg.user { background: #e8f0ff44; }\n    .msg.assistant { background: #f3f3f344; }\n    .meta { display: flex; gap: 10px; align-items: baseline; font-size: 12px; opacity: 0.8; margin-bottom: 6px; }\n    .who { font-weight: 600; }\n    .ts { font-variant-numeric: tabular-nums; }\n    .src { font-size: 11px; opacity: 0.8; }\n    pre { white-space: pre-wrap; margin: 0; }\n    mark { background: #ffeb3b80; padding: 0 2px; border-radius: 3px; }\n    a { color: inherit; }\n  </style>");
            html.WriteLine("</head>");
            html.WriteLine("<body>");
            html.WriteLine($"  <h1>Transcript: {WebUtility.HtmlEncode(opts.Channel)}:{WebUtility.HtmlEncode(opts.ChatId)}</h1>");

            foreach (var l in ordered)
            {
                var text = NormalizeForDisplay(l.Text);
                var who = string.IsNullOrWhiteSpace(l.Speaker) ? (l.Role == "user" ? "User" : "Assistant") : l.Speaker;
                var srcLabel = $"{l.SourceKind}:{l.SourceId}";
                var srcHref = BuildSourceHref(l, rawUserPath, rawLlmPath);

                html.WriteLine($"  <div class=\"msg {WebUtility.HtmlEncode(l.Role)}\" data-source=\"{WebUtility.HtmlEncode(l.SourceKind)}\" data-id=\"{WebUtility.HtmlEncode(l.SourceId)}\">");
                html.WriteLine("    <div class=\"meta\">");
                html.WriteLine($"      <span class=\"who\">{WebUtility.HtmlEncode(who)}</span>");
                html.WriteLine($"      <span class=\"ts\">{WebUtility.HtmlEncode(l.Ts.ToString("yyyy-MM-dd HH:mm"))}</span>");
                if (!string.IsNullOrWhiteSpace(srcHref))
                    html.WriteLine($"      <a class=\"src\" href=\"{WebUtility.HtmlEncode(srcHref)}\">{WebUtility.HtmlEncode(srcLabel)}</a>");
                else
                    html.WriteLine($"      <span class=\"src\">{WebUtility.HtmlEncode(srcLabel)}</span>");
                html.WriteLine("    </div>");
                html.WriteLine($"    <pre class=\"text\">{WebUtility.HtmlEncode(text)}</pre>");
                html.WriteLine("  </div>");
            }

            html.WriteLine("  <script>");
            html.WriteLine("    (function(){");
            html.WriteLine("      const params = new URLSearchParams(window.location.search);");
            html.WriteLine("      const hl = params.get('hl');");
            html.WriteLine("      if (!hl) return;");
            html.WriteLine("      const escapeHtml = (s) => String(s).replace(/[&<>\"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;','\'':'&#39;'}[c]));");
            html.WriteLine("      const pres = document.querySelectorAll('pre.text');");
            html.WriteLine("      for (const pre of pres) {");
            html.WriteLine("        const t = pre.textContent || '';" );
            html.WriteLine("        const idx = t.indexOf(hl);");
            html.WriteLine("        if (idx >= 0) {");
            html.WriteLine("          const before = t.substring(0, idx);");
            html.WriteLine("          const mid = t.substring(idx, idx + hl.length);");
            html.WriteLine("          const after = t.substring(idx + hl.length);");
            html.WriteLine("          pre.innerHTML = escapeHtml(before) + '<mark>' + escapeHtml(mid) + '</mark>' + escapeHtml(after);");
            html.WriteLine("          pre.scrollIntoView({behavior: 'smooth', block: 'center'});");
            html.WriteLine("          break;");
            html.WriteLine("        }");
            html.WriteLine("      }");
            html.WriteLine("    })();");
            html.WriteLine("  </script>");
            html.WriteLine("</body>");
            html.WriteLine("</html>");
        }
    }

    private static bool LooksLikeAutomation(string text)
    {
        if (string.IsNullOrWhiteSpace(text)) return false;
        var t = text.TrimStart();

        // memory pipeline / cron chatter
        if (t.StartsWith("Ingest chat_", StringComparison.OrdinalIgnoreCase)) return true;
        if (t.StartsWith("Derived transcript updated", StringComparison.OrdinalIgnoreCase)) return true;
        if (t.StartsWith("Telegram ingest job", StringComparison.OrdinalIgnoreCase)) return true;

        return false;
    }

    private static bool LooksLikeAutomationSpeaker(string? speaker)
    {
        if (string.IsNullOrWhiteSpace(speaker)) return false;
        var s = speaker.Trim();
        if (s.Equals("openclaw", StringComparison.OrdinalIgnoreCase)) return true;
        if (s.StartsWith("System", StringComparison.OrdinalIgnoreCase)) return true;
        return false;
    }

    private static string NormalizeSpeaker(string? speaker, string role)
    {
        if (string.IsNullOrWhiteSpace(speaker))
            return role == "assistant" ? "Assistant" : "User";

        var s = speaker.Trim();

        // Your nickname
        if (s.IndexOf("Ming", StringComparison.OrdinalIgnoreCase) >= 0) return "Ming";
        if (s.IndexOf("Euming", StringComparison.OrdinalIgnoreCase) >= 0) return "Ming";
        if (s.IndexOf("5819070869", StringComparison.OrdinalIgnoreCase) >= 0) return "Ming";

        return s;
    }

    public static string NormalizeForDisplay(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return "";
        var text = s.Replace("\r\n", "\n");

        // If the stored text still looks like a wrapped OpenClaw/Telegram prefix, strip it.
        // (This helps older backfilled rawUser lines.)
        var close = text.IndexOf(']');
        if (close >= 0)
        {
            var colon = text.IndexOf(':', close + 1);
            if (colon >= 0)
            {
                text = text.Substring(colon + 1);
            }
        }

        // Drop any embedded message_id lines.
        var lines = text.Split('\n');
        var kept = new List<string>(lines.Length);
        foreach (var line in lines)
        {
            if (line.TrimStart().StartsWith("[message_id:", StringComparison.OrdinalIgnoreCase))
                continue;
            kept.Add(line);
        }

        return string.Join("\n", kept).Trim();
    }

    public static string NormalizeForMatch(string s)
    {
        return NormalizeForDisplay(s).Trim();
    }

    private static string? BuildSourceHref(Line line, string rawUserPath, string rawLlmPath)
    {
        var sourcePath = line.SourceKind == "rawUser" ? rawUserPath : rawLlmPath;
        if (string.IsNullOrWhiteSpace(sourcePath)) return null;
        if (!File.Exists(sourcePath)) return null;

        try
        {
            var uri = new Uri(sourcePath);
            var href = uri.AbsoluteUri;
            if (!string.IsNullOrWhiteSpace(line.SourceId))
                href += $"#id={Uri.EscapeDataString(line.SourceId)}";
            return href;
        }
        catch
        {
            return null;
        }
    }
}
