using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.Json;
using CardBinder.Core;

namespace MemoryCtl;

public static class MemoryJsonlReader
{
    public static MemoryDb Load(string path)
    {
        using var fs = File.OpenRead(path);
        using var sr = new StreamReader(fs);
        return Load(sr);
    }

    public static MemoryDb Load(TextReader reader)
    {
        var core = new CardBinderCore();
        var db = new MemoryDb(core);

        string? line;
        bool sawHeader = false;

        while ((line = reader.ReadLine()) != null)
        {
            if (string.IsNullOrWhiteSpace(line)) continue;

            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;

            if (!root.TryGetProperty("type", out var typeEl) || typeEl.ValueKind != JsonValueKind.String)
                throw new FormatException("Missing 'type' field in JSONL record.");

            var type = typeEl.GetString()!;

            switch (type)
            {
                case "format":
                {
                    var name = root.GetProperty("name").GetString();
                    var version = root.GetProperty("version").GetInt32();
                    if (!string.Equals(name, "card-binder", StringComparison.Ordinal))
                        throw new FormatException($"Unexpected format name '{name}'.");
                    if (version != 1)
                        throw new FormatException($"Unsupported format version '{version}'.");
                    sawHeader = true;
                    break;
                }

                case "card":
                {
                    EnsureHeader(sawHeader);
                    var id = Guid.Parse(root.GetProperty("id").GetString()!);
                    var stateStr = root.GetProperty("state").GetString()!;
                    var state = Enum.Parse<CardState>(stateStr, ignoreCase: true);
                    string? reason = null;
                    if (root.TryGetProperty("state_reason", out var rEl) && rEl.ValueKind == JsonValueKind.String)
                        reason = rEl.GetString();
                    core.UpsertCard(new CardId(id), state, reason);
                    break;
                }

                case "binder":
                {
                    EnsureHeader(sawHeader);
                    var id = Guid.Parse(root.GetProperty("id").GetString()!);
                    var name = root.GetProperty("name").GetString() ?? "Binder";
                    core.UpsertBinder(new BinderId(id), name);
                    break;
                }

                case "taglink":
                {
                    EnsureHeader(sawHeader);
                    var cardId = Guid.Parse(root.GetProperty("card_id").GetString()!);
                    var binderId = Guid.Parse(root.GetProperty("binder_id").GetString()!);
                    var meta = ParseMeta(root);

                    // ensure endpoints exist
                    if (!core.CardExists(new CardId(cardId))) core.UpsertCard(new CardId(cardId));
                    if (!core.BinderExists(new BinderId(binderId))) core.UpsertBinder(new BinderId(binderId), "Imported");

                    core.Link(new CardId(cardId), new BinderId(binderId), meta);
                    break;
                }

                // Optional extension: payload text for immediate usefulness.
                case "card_payload":
                {
                    EnsureHeader(sawHeader);
                    var cardId = Guid.Parse(root.GetProperty("card_id").GetString()!);
                    string? title = TryGetString(root, "title");
                    string? text = TryGetString(root, "text");
                    string? source = TryGetString(root, "source");
                    DateTimeOffset? updatedAt = null;
                    var updatedAtStr = TryGetString(root, "updated_at");
                    if (!string.IsNullOrWhiteSpace(updatedAtStr))
                    {
                        if (DateTimeOffset.TryParse(updatedAtStr, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
                            updatedAt = dto;
                    }

                    db.PayloadByCardId[cardId] = new CardPayload(cardId, title, text, source, updatedAt);

                    // Ensure card exists.
                    if (!core.CardExists(new CardId(cardId))) core.UpsertCard(new CardId(cardId));
                    break;
                }

                default:
                    // Forward-compatible behavior: ignore unknown record types.
                    break;
            }
        }

        if (!sawHeader)
            throw new FormatException("Missing format header record.");

        return db;
    }

    private static TagLinkMeta ParseMeta(JsonElement root)
    {
        // Default meta (matches core default behavior).
        float relevance = 0.5f;
        string? reason = null;
        string? addedBy = null;
        DateTimeOffset? createdAt = null;

        if (root.TryGetProperty("meta", out var metaEl) && metaEl.ValueKind == JsonValueKind.Object)
        {
            if (metaEl.TryGetProperty("Relevance", out var rEl) && rEl.ValueKind == JsonValueKind.Number)
                relevance = (float)rEl.GetDouble();

            if (metaEl.TryGetProperty("Reason", out var rsEl) && rsEl.ValueKind == JsonValueKind.String)
                reason = rsEl.GetString();

            if (metaEl.TryGetProperty("AddedBy", out var abEl) && abEl.ValueKind == JsonValueKind.String)
                addedBy = abEl.GetString();

            if (metaEl.TryGetProperty("CreatedAt", out var caEl) && caEl.ValueKind == JsonValueKind.String)
            {
                if (DateTimeOffset.TryParse(caEl.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
                    createdAt = dto;
            }
        }

        return new TagLinkMeta(Relevance: relevance, Reason: reason, AddedBy: addedBy, CreatedAt: createdAt);
    }

    private static string? TryGetString(JsonElement obj, string prop)
    {
        if (obj.TryGetProperty(prop, out var el) && el.ValueKind == JsonValueKind.String)
            return el.GetString();
        return null;
    }

    private static void EnsureHeader(bool sawHeader)
    {
        if (!sawHeader)
            throw new FormatException("Format header must be the first record.");
    }
}
