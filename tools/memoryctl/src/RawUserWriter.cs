using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace MemoryCtl;

internal static class RawUserWriter
{
    public static bool AppendInbound(string rawUserPath, ChatEvent evt)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(rawUserPath) ?? ".");

        // Best-effort tail dedupe by message id.
        var key = evt.MessageId;
        if (File.Exists(rawUserPath))
        {
            var tail = TailLines(rawUserPath, maxLines: 80);
            foreach (var line in tail)
            {
                if (line.Contains('"' + key + '"', StringComparison.Ordinal))
                    return false;
            }
        }

        var obj = new Dictionary<string, object?>
        {
            ["type"] = "chat_event",
            ["channel"] = evt.Channel,
            ["chat_id"] = evt.ChatId,
            ["message_id"] = evt.MessageId,
            ["ts"] = evt.Ts.ToString("o"),
            ["author"] = evt.Author,
            ["direction"] = evt.Direction,
            ["text"] = evt.Text,
            ["source"] = "session"
        };

        File.AppendAllText(rawUserPath, JsonSerializer.Serialize(obj) + "\n");
        return true;
    }

    private static IReadOnlyList<string> TailLines(string path, int maxLines)
    {
        var lines = File.ReadAllLines(path);
        if (lines.Length <= maxLines) return lines;
        var result = new string[maxLines];
        Array.Copy(lines, lines.Length - maxLines, result, 0, maxLines);
        return result;
    }
}
