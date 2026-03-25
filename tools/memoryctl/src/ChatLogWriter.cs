using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace MemoryCtl;

internal static class ChatLogWriter
{
    public static bool AppendChatEvent(string chatlogPath, ChatEvent evt, int dedupeTailLines = 200)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(chatlogPath) ?? ".");

        var key = MakeKey(evt);

        if (File.Exists(chatlogPath) && ContainsKeyInTail(chatlogPath, key, dedupeTailLines))
            return false;

        using var sw = File.AppendText(chatlogPath);
        sw.WriteLine(JsonSerializer.Serialize(new
        {
            type = "chat_event",
            channel = evt.Channel,
            chat_id = evt.ChatId,
            message_id = evt.MessageId,
            ts = evt.Ts.ToString("o"),
            author = evt.Author,
            direction = evt.Direction,
            text = evt.Text
        }));

        return true;
    }

    private static string MakeKey(ChatEvent evt) => $"{evt.Channel}|{evt.ChatId}|{evt.MessageId}";

    private static bool ContainsKeyInTail(string path, string key, int tailLines)
    {
        // Cheap-ish dedupe: scan only last N lines. Assumes message_ids are roughly increasing.
        var q = new Queue<string>(tailLines);
        foreach (var line in File.ReadLines(path))
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            q.Enqueue(line);
            while (q.Count > tailLines) q.Dequeue();
        }

        foreach (var line in q)
        {
            try
            {
                using var doc = JsonDocument.Parse(line);
                var root = doc.RootElement;
                if (!root.TryGetProperty("type", out var t) || t.GetString() != "chat_event") continue;

                var channel = root.GetProperty("channel").GetString();
                var chatId = root.GetProperty("chat_id").GetString();
                var msgId = root.GetProperty("message_id").GetString();
                var k = $"{channel}|{chatId}|{msgId}";
                if (string.Equals(k, key, StringComparison.Ordinal))
                    return true;
            }
            catch
            {
                // ignore malformed tail lines
            }
        }

        return false;
    }
}
