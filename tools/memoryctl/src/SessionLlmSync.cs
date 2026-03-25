using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace MemoryCtl;

internal static class SessionLlmSync
{
    public sealed record SyncResult(int SessionsSeen, int AssistantMessagesSeen, int Appended, int SkippedDupes);

    private static readonly Regex MessageIdRe = new(@"\[message_id:\s*(\d+)\]", RegexOptions.Compiled);

    public static SyncResult SyncTelegramSessionsToRawLlm(
        string sessionsJsonPath,
        string rawLlmDir,
        string cursorDir,
        bool skipNoReply = true)
    {
        Directory.CreateDirectory(rawLlmDir);
        Directory.CreateDirectory(cursorDir);

        using var doc = JsonDocument.Parse(File.ReadAllText(sessionsJsonPath));
        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            throw new FormatException("sessions.json must be a JSON object");

        int sessionsSeen = 0;
        int assistantSeen = 0;
        int appended = 0;
        int dupes = 0;

        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            var sessionKey = prop.Name;
            var obj = prop.Value;

            if (!TryGetString(obj, "channel", out var channel) || !string.Equals(channel, "telegram", StringComparison.OrdinalIgnoreCase))
                continue;

            if (!TryGetString(obj, "chatType", out var chatType) || !(string.Equals(chatType, "group", StringComparison.OrdinalIgnoreCase) || string.Equals(chatType, "direct", StringComparison.OrdinalIgnoreCase)))
                continue;

            if (!TryGetString(obj, "sessionFile", out var sessionFile) || string.IsNullOrWhiteSpace(sessionFile) || !File.Exists(sessionFile))
                continue;

            string chatId;
            if (TryGetString(obj, "groupId", out var groupId) && !string.IsNullOrWhiteSpace(groupId))
                chatId = groupId!;
            else if (TryGetDeliveryTo(obj, out var to) && TryParseTelegramToChatId(to, out var directChatId))
                chatId = directChatId;
            else
                continue;

            sessionsSeen++;

            var cursorPath = Path.Combine(cursorDir, SafeFileName(sessionKey) + ".cursor.json");
            var cursor = CursorStore.Load(cursorPath);

            string? lastUserMessageId = null;
            DateTimeOffset? maxTsProcessed = cursor.LastTs;

            foreach (var line in File.ReadLines(sessionFile))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                using var lineDoc = JsonDocument.Parse(line);
                var root = lineDoc.RootElement;
                if (root.ValueKind != JsonValueKind.Object) continue;

                if (!TryGetString(root, "type", out var type) || !string.Equals(type, "message", StringComparison.Ordinal))
                    continue;

                // Cursor: we use the transcript line's "timestamp" as a monotonic-ish checkpoint.
                if (!TryGetString(root, "timestamp", out var tsStr) || !DateTimeOffset.TryParse(tsStr, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var ts))
                    continue;

                if (cursor.LastTs != null && ts <= cursor.LastTs.Value)
                    continue;

                if (maxTsProcessed == null || ts > maxTsProcessed.Value)
                    maxTsProcessed = ts;

                if (!root.TryGetProperty("message", out var msg) || msg.ValueKind != JsonValueKind.Object)
                    continue;

                if (!TryGetString(msg, "role", out var role))
                    continue;

                var text = ExtractFirstText(msg);
                if (text == null) continue;

                if (string.Equals(role, "user", StringComparison.OrdinalIgnoreCase))
                {
                    var m = MessageIdRe.Match(text);
                    if (m.Success)
                        lastUserMessageId = m.Groups[1].Value;
                    continue;
                }

                if (!string.Equals(role, "assistant", StringComparison.OrdinalIgnoreCase))
                    continue;

                assistantSeen++;

                if (skipNoReply && string.Equals(text.Trim(), "NO_REPLY", StringComparison.Ordinal))
                    continue;

                var completionId = ExtractTextSignature(msg) ?? GuidUtil.FromKey("completion:" + sessionKey + ":" + ts.ToString("o")).ToString();

                var outPath = Path.Combine(rawLlmDir, $"chat_{chatId}.jsonl");
                var rec = new LlmCompletion(
                    Channel: "telegram",
                    ChatId: chatId,
                    CompletionMessageId: completionId,
                    Ts: ts,
                    ParentMessageId: lastUserMessageId,
                    Model: TryGetString(msg, "model", out var model) ? model : null,
                    Text: text);

                var ok = RawLlmWriter.AppendCompletion(outPath, rec);
                if (ok) appended++; else dupes++;
            }

            // Save cursor as the last processed transcript timestamp.
            // NOTE: We don't track line numbers because session logs may get compacted/rewritten.
            CursorStore.Save(cursorPath, new ChatCursor(0, maxTsProcessed, null));
        }

        return new SyncResult(sessionsSeen, assistantSeen, appended, dupes);
    }

    private static bool TryGetDeliveryTo(JsonElement sessionObj, out string? to)
    {
        to = null;
        if (!sessionObj.TryGetProperty("deliveryContext", out var dc) || dc.ValueKind != JsonValueKind.Object)
            return false;
        return TryGetString(dc, "to", out to);
    }

    internal static bool TryParseTelegramToChatIdForUserSync(string? to, out string chatId) => TryParseTelegramToChatId(to, out chatId);

    internal static bool TryParseTelegramToChatId(string? to, out string chatId)
    {
        chatId = "";
        if (string.IsNullOrWhiteSpace(to)) return false;
        // expected: telegram:-5229501860 or telegram:5819070869
        var s = to.Trim();
        if (s.StartsWith("telegram:", StringComparison.OrdinalIgnoreCase))
            s = s.Substring("telegram:".Length);
        if (s.StartsWith("group:", StringComparison.OrdinalIgnoreCase))
            s = s.Substring("group:".Length);
        if (!Regex.IsMatch(s, "^-?\\d+$")) return false;
        chatId = s;
        return true;
    }

    internal static string? ExtractFirstTextForSync(JsonElement msg) => ExtractFirstText(msg);

    internal static string? ExtractFirstText(JsonElement msg)
    {
        if (!msg.TryGetProperty("content", out var content) || content.ValueKind != JsonValueKind.Array)
            return null;

        foreach (var item in content.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object) continue;
            if (!TryGetString(item, "type", out var t)) continue;
            if (string.Equals(t, "text", StringComparison.OrdinalIgnoreCase) && TryGetString(item, "text", out var text))
                return text;
        }

        return null;
    }

    private static string? ExtractTextSignature(JsonElement msg)
    {
        if (!msg.TryGetProperty("content", out var content) || content.ValueKind != JsonValueKind.Array)
            return null;

        foreach (var item in content.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object) continue;
            if (TryGetString(item, "textSignature", out var sig) && !string.IsNullOrWhiteSpace(sig))
                return sig;
        }

        return null;
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
        // keep it boring
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
