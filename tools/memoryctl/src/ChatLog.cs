using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.Json;

namespace MemoryCtl;

internal static class ChatLog
{
    public static IEnumerable<(int lineNo, ChatEvent evt)> ReadEvents(
        string path,
        Action<int, string, Exception>? onMalformedLine = null)
    {
        using var fs = File.OpenRead(path);
        using var sr = new StreamReader(fs);

        string? line;
        int lineNo = 0;

        while ((line = sr.ReadLine()) != null)
        {
            lineNo++;
            if (string.IsNullOrWhiteSpace(line)) continue;

            if (TryParseChatEventLine(line, out var evt, out var parseError))
            {
                if (evt != null)
                    yield return (lineNo, evt);
                continue;
            }

            // Malformed line (JSON / required field / timestamp): log and continue.
            var preview = line.Length <= 240 ? line : line.Substring(0, 240) + "...";
            onMalformedLine?.Invoke(lineNo, preview, parseError ?? new FormatException("Unknown parse error"));
        }
    }

    /// <summary>
    /// Reads tool_event lines from a chatlog JSONL file.
    /// </summary>
    public static IEnumerable<(int lineNo, ToolCallEvent evt)> ReadToolEvents(
        string path,
        Action<int, string, Exception>? onMalformedLine = null)
    {
        using var fs = File.OpenRead(path);
        using var sr = new StreamReader(fs);

        string? line;
        int lineNo = 0;

        while ((line = sr.ReadLine()) != null)
        {
            lineNo++;
            if (string.IsNullOrWhiteSpace(line)) continue;

            if (TryParseToolEventLine(line, out var evt, out var parseError))
            {
                if (evt != null)
                    yield return (lineNo, evt);
                continue;
            }

            // Not a tool_event or malformed — skip silently (chat events handled elsewhere).
        }
    }

    // Returns:
    // - true, evt!=null => parsed a tool_event
    // - true, evt==null => valid JSON but not a tool_event (ignored)
    // - false => malformed / invalid
    internal static bool TryParseToolEventLine(string line, out ToolCallEvent? evt, out Exception? error)
    {
        evt = null;
        error = null;

        try
        {
            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;

            if (!TryGetString(root, "type", out var type) || !string.Equals(type, "tool_event", StringComparison.Ordinal))
                return true; // not a tool_event, skip

            var channel   = RequiredString(root, "channel");
            var chatId    = RequiredString(root, "chat_id");
            var toolUseId = RequiredString(root, "tool_use_id");
            var tsStr     = RequiredString(root, "ts");
            if (!DateTimeOffset.TryParse(tsStr, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var ts))
                throw new FormatException($"Invalid ts: '{tsStr}'");

            var toolName = RequiredString(root, "tool_name");

            // Input can be an object or a string; serialize to JSON string for storage.
            string inputJson = "{}";
            if (root.TryGetProperty("input", out var inputEl))
                inputJson = inputEl.GetRawText();

            TryGetString(root, "result_preview", out var resultPreview);

            bool isError = false;
            if (root.TryGetProperty("is_error", out var isErrEl) && isErrEl.ValueKind is JsonValueKind.True or JsonValueKind.False)
                isError = isErrEl.GetBoolean();

            evt = new ToolCallEvent(channel, chatId, toolUseId, ts, toolName, inputJson, resultPreview, isError);
            return true;
        }
        catch (Exception ex) when (ex is JsonException || ex is FormatException)
        {
            error = ex;
            return false;
        }
    }

    // Returns:
    // - true, evt!=null => parsed a chat_event
    // - true, evt==null => valid JSON but not a chat_event (ignored)
    // - false => malformed / invalid
    private static bool TryParseChatEventLine(string line, out ChatEvent? evt, out Exception? error)
    {
        evt = null;
        error = null;

        try
        {
            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;

            if (!TryGetString(root, "type", out var type) || !string.Equals(type, "chat_event", StringComparison.Ordinal))
                return true;

            var channel = RequiredString(root, "channel");
            var chatId = RequiredString(root, "chat_id");
            var messageId = RequiredString(root, "message_id");
            var tsStr = RequiredString(root, "ts");
            if (!DateTimeOffset.TryParse(tsStr, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var ts))
                throw new FormatException($"Invalid ts: '{tsStr}'");

            var direction = RequiredString(root, "direction");
            var text = RequiredString(root, "text");

            TryGetString(root, "author", out var author);
            TryGetString(root, "slug", out var slug);   // optional, falls back to null

            var tokensIn          = TryGetInt(root, "tokens_in");
            var tokensOut         = TryGetInt(root, "tokens_out");
            var tokensCacheRead   = TryGetInt(root, "tokens_cache_read");
            var tokensCacheCreate = TryGetInt(root, "tokens_cache_create");

            evt = new ChatEvent(channel, chatId, messageId, ts, author, direction, text, slug,
                tokensIn, tokensOut, tokensCacheRead, tokensCacheCreate);
            return true;
        }
        catch (Exception ex) when (ex is JsonException || ex is FormatException)
        {
            error = ex;
            return false;
        }
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

    private static int TryGetInt(JsonElement root, string key)
        => root.TryGetProperty(key, out var el) && el.TryGetInt32(out var v) ? v : 0;

    private static string RequiredString(JsonElement obj, string prop)
    {
        if (!TryGetString(obj, prop, out var v) || string.IsNullOrWhiteSpace(v))
            throw new FormatException($"Missing/invalid '{prop}' field in chat_event.");
        return v!;
    }
}
