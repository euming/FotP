using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.Json;

namespace MemoryCtl;

internal static class RawLlmLog
{
    public static IEnumerable<(int lineNo, LlmCompletion completion)> ReadCompletions(
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

            if (TryParse(line, out var rec, out var err))
            {
                if (rec != null)
                    yield return (lineNo, rec);
                continue;
            }

            var preview = line.Length <= 240 ? line : line.Substring(0, 240) + "...";
            onMalformedLine?.Invoke(lineNo, preview, err ?? new FormatException("Unknown parse error"));
        }
    }

    private static bool TryParse(string line, out LlmCompletion? rec, out Exception? error)
    {
        rec = null;
        error = null;

        try
        {
            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;

            if (!TryGetString(root, "type", out var type) || !string.Equals(type, "llm_completion", StringComparison.Ordinal))
                return true;

            var channel = RequiredString(root, "channel");
            var chatId = RequiredString(root, "chat_id");
            var completionMessageId = RequiredString(root, "completion_message_id");
            var tsStr = RequiredString(root, "ts");
            if (!DateTimeOffset.TryParse(tsStr, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var ts))
                throw new FormatException($"Invalid ts: '{tsStr}'");

            var text = RequiredString(root, "text");

            TryGetString(root, "parent_message_id", out var parent);
            TryGetString(root, "model", out var model);

            rec = new LlmCompletion(channel, chatId, completionMessageId, ts, parent, model, text);
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

    private static string RequiredString(JsonElement obj, string prop)
    {
        if (!TryGetString(obj, prop, out var v) || string.IsNullOrWhiteSpace(v))
            throw new FormatException($"Missing/invalid '{prop}' field in llm_completion.");
        return v!;
    }
}
