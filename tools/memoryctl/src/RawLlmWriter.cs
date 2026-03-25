using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace MemoryCtl;

internal static class RawLlmWriter
{
    public static bool AppendCompletion(string rawLlmPath, LlmCompletion rec)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(rawLlmPath) ?? ".");

        // Best-effort tail dedupe: check last ~50 lines for same completion id.
        var key = rec.CompletionMessageId;
        if (File.Exists(rawLlmPath))
        {
            var tail = TailLines(rawLlmPath, maxLines: 50);
            foreach (var line in tail)
            {
                if (line.Contains(key, StringComparison.Ordinal))
                    return false;
            }
        }

        var obj = new Dictionary<string, object?>
        {
            ["type"] = "llm_completion",
            ["channel"] = rec.Channel,
            ["chat_id"] = rec.ChatId,
            ["completion_message_id"] = rec.CompletionMessageId,
            ["parent_message_id"] = rec.ParentMessageId,
            ["ts"] = rec.Ts.ToString("o"),
            ["model"] = rec.Model,
            ["text"] = rec.Text,
        };

        File.AppendAllText(rawLlmPath, JsonSerializer.Serialize(obj) + "\n");
        return true;
    }

    private static IReadOnlyList<string> TailLines(string path, int maxLines)
    {
        // Simple implementation: read all if file is small.
        var lines = File.ReadAllLines(path);
        if (lines.Length <= maxLines) return lines;
        var result = new string[maxLines];
        Array.Copy(lines, lines.Length - maxLines, result, 0, maxLines);
        return result;
    }
}
