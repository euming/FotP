using System;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace MemoryCtl;

internal static class SessionUserSync
{
    public sealed record SyncResult(int SessionsSeen, int UserMessagesSeen, int Appended, int SkippedDupes);

    private static readonly Regex MessageIdRe = new(@"\[message_id:\s*(\d+)\]", RegexOptions.Compiled);

    public static SyncResult SyncTelegramSessionsToRawUser(
        string sessionsJsonPath,
        string rawUserDir,
        string cursorDir)
    {
        Directory.CreateDirectory(rawUserDir);
        Directory.CreateDirectory(cursorDir);

        using var doc = JsonDocument.Parse(File.ReadAllText(sessionsJsonPath));
        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            throw new FormatException("sessions.json must be a JSON object");

        int sessionsSeen = 0;
        int userSeen = 0;
        int appended = 0;
        int dupes = 0;

        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            var sessionKey = prop.Name;
            var obj = prop.Value;

            if (!TryGetString(obj, "channel", out var channel) || !string.Equals(channel, "telegram", StringComparison.OrdinalIgnoreCase))
                continue;

            if (!TryGetString(obj, "sessionFile", out var sessionFile) || string.IsNullOrWhiteSpace(sessionFile) || !File.Exists(sessionFile))
                continue;

            string chatId;
            if (TryGetString(obj, "groupId", out var groupId) && !string.IsNullOrWhiteSpace(groupId))
                chatId = groupId!;
            else if (TryGetDeliveryTo(obj, out var to) && SessionLlmSync.TryParseTelegramToChatIdForUserSync(to, out var directChatId))
                chatId = directChatId;
            else
                continue;

            sessionsSeen++;

            var cursorPath = Path.Combine(cursorDir, SafeFileName(sessionKey) + ".cursor.json");
            var cursor = CursorStore.Load(cursorPath);
            DateTimeOffset? maxTsProcessed = cursor.LastTs;

            foreach (var line in File.ReadLines(sessionFile))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                using var lineDoc = JsonDocument.Parse(line);
                var root = lineDoc.RootElement;
                if (root.ValueKind != JsonValueKind.Object) continue;

                if (!TryGetString(root, "type", out var type) || !string.Equals(type, "message", StringComparison.Ordinal))
                    continue;

                if (!TryGetString(root, "timestamp", out var tsStr) || !DateTimeOffset.TryParse(tsStr, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var ts))
                    continue;

                if (cursor.LastTs != null && ts <= cursor.LastTs.Value)
                    continue;

                if (maxTsProcessed == null || ts > maxTsProcessed.Value)
                    maxTsProcessed = ts;

                if (!root.TryGetProperty("message", out var msg) || msg.ValueKind != JsonValueKind.Object)
                    continue;

                if (!TryGetString(msg, "role", out var role) || !string.Equals(role, "user", StringComparison.OrdinalIgnoreCase))
                    continue;

                var text = SessionLlmSync.ExtractFirstTextForSync(msg);
                if (text == null) continue;

                var m = MessageIdRe.Match(text);
                if (!m.Success) continue;

                var messageId = m.Groups[1].Value;

                // Attempt to pull author name from the bracketed prefix.
                // Example: [Telegram ...] Euming (Ming) Lee (5819070869): ...
                var author = "user";
                var authorMatch = Regex.Match(text, @"\]\s*([^:]+):");
                if (authorMatch.Success)
                    author = authorMatch.Groups[1].Value.Trim();

                // Extract the message body after the ':' that follows the bracketed prefix.
                // Example:
                // [Telegram ...] Name (id): <BODY>\n[message_id: 123]
                var body = ExtractBody(text);

                userSeen++;

                var outPath = Path.Combine(rawUserDir, $"chat_{chatId}.jsonl");
                var evt = new ChatEvent(
                    Channel: "telegram",
                    ChatId: chatId,
                    MessageId: messageId,
                    Ts: ts,
                    Author: author,
                    Direction: "in",
                    Text: body);

                var ok = RawUserWriter.AppendInbound(outPath, evt);
                if (ok) appended++; else dupes++;
            }

            CursorStore.Save(cursorPath, new ChatCursor(0, maxTsProcessed, null));
        }

        return new SyncResult(sessionsSeen, userSeen, appended, dupes);
    }

    private static string ExtractBody(string wrapped)
    {
        if (string.IsNullOrWhiteSpace(wrapped)) return "";

        // Find ':' after the first closing bracket.
        var close = wrapped.IndexOf(']');
        var start = close >= 0 ? close + 1 : 0;
        var colon = wrapped.IndexOf(':', start);
        string body = colon >= 0 && colon + 1 < wrapped.Length
            ? wrapped.Substring(colon + 1)
            : wrapped;

        body = body.Replace("\r\n", "\n");

        // Drop trailing message_id line(s)
        var lines = body.Split('\n');
        var kept = new System.Collections.Generic.List<string>(lines.Length);
        foreach (var line in lines)
        {
            if (line.TrimStart().StartsWith("[message_id:", StringComparison.OrdinalIgnoreCase))
                continue;
            kept.Add(line);
        }

        return string.Join("\n", kept).Trim();
    }

    private static bool TryGetDeliveryTo(JsonElement sessionObj, out string? to)
    {
        to = null;
        if (!sessionObj.TryGetProperty("deliveryContext", out var dc) || dc.ValueKind != JsonValueKind.Object)
            return false;
        return TryGetString(dc, "to", out to);
    }

    private static bool TryGetString(JsonElement obj, string prop, out string? value)
    {
        value = null;
        if (obj.ValueKind != JsonValueKind.Object) return false;
        if (!obj.TryGetProperty(prop, out var el)) return false;
        if (el.ValueKind != JsonValueKind.String) return false;
        value = el.GetString();
        return true;
    }

    private static string SafeFileName(string s)
    {
        var chars = s.ToCharArray();
        for (int i = 0; i < chars.Length; i++)
        {
            var c = chars[i];
            if (char.IsLetterOrDigit(c) || c == '_' || c == '-' || c == '.')
                continue;
            chars[i] = '_';
        }
        return new string(chars);
    }
}
