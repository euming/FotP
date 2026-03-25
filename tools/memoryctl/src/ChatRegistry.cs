using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace MemoryCtl;

internal static class ChatRegistry
{
    public sealed record Entry(string Label, string Channel, string ChatId);

    public static IReadOnlyList<Entry> Load(string path)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var root = doc.RootElement;

        if (!root.TryGetProperty("chats", out var chats) || chats.ValueKind != JsonValueKind.Array)
            return Array.Empty<Entry>();

        var list = new List<Entry>();
        foreach (var c in chats.EnumerateArray())
        {
            var label = GetString(c, "label");
            var channel = GetString(c, "channel");
            var chatId = GetString(c, "chat_id");
            if (string.IsNullOrWhiteSpace(label) || string.IsNullOrWhiteSpace(channel) || string.IsNullOrWhiteSpace(chatId))
                continue;
            list.Add(new Entry(label!, channel!, chatId!));
        }
        return list;
    }

    public static bool TryResolveChatId(IReadOnlyList<Entry> entries, string channel, string labelOrId, out string chatId)
    {
        chatId = "";
        if (string.IsNullOrWhiteSpace(labelOrId)) return false;

        // Already an id?
        var t = labelOrId.Trim();
        var isNumeric = true;
        foreach (var ch in t)
        {
            if (ch == '-') continue;
            if (!char.IsDigit(ch)) { isNumeric = false; break; }
        }
        if (isNumeric)
        {
            chatId = t;
            return true;
        }

        foreach (var e in entries)
        {
            if (!string.Equals(e.Channel, channel, StringComparison.OrdinalIgnoreCase))
                continue;
            if (string.Equals(e.Label, labelOrId, StringComparison.OrdinalIgnoreCase))
            {
                chatId = e.ChatId;
                return true;
            }
        }

        return false;
    }

    private static string? GetString(JsonElement obj, string prop)
    {
        if (obj.ValueKind != JsonValueKind.Object) return null;
        if (!obj.TryGetProperty(prop, out var el)) return null;
        if (el.ValueKind != JsonValueKind.String) return null;
        return el.GetString();
    }
}
