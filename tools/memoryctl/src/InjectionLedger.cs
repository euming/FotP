using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CardBinder.Core;

namespace MemoryCtl;

internal static class InjectionLedger
{
    public sealed record Entry(
        DateTimeOffset Ts,
        string Channel,
        string ChatId,
        string ChatLabel,
        string BinderName,
        IReadOnlyList<Guid> CardIds,
        int RenderedChars,
        string RenderedSha256,
        string Reason);

    public static void Append(string ledgerPath, Entry entry)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(ledgerPath) ?? ".");

        var obj = new Dictionary<string, object?>
        {
            ["type"] = "injection_ledger",
            ["ts"] = entry.Ts.ToString("o", CultureInfo.InvariantCulture),
            ["channel"] = entry.Channel,
            ["chat_id"] = entry.ChatId,
            ["chat_label"] = entry.ChatLabel,
            ["memAnchor"] = entry.BinderName,
            ["card_ids"] = entry.CardIds.Select(x => x.ToString()).ToArray(),
            ["rendered_chars"] = entry.RenderedChars,
            ["rendered_sha256"] = entry.RenderedSha256,
            ["reason"] = entry.Reason
        };

        File.AppendAllText(ledgerPath, JsonSerializer.Serialize(obj) + "\n");
    }

    public static string Sha256Hex(string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text ?? "");
        var hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    public static IReadOnlyList<Guid> CollectCardIdsForBinder(MemoryDb db, string binderName)
    {
        MemAnchorId? memAnchor = null;
        foreach (var b in db.Core.AllBinders)
        {
            if (!db.Core.TryGetBinderName(b, out var n) || string.IsNullOrWhiteSpace(n)) continue;
            if (string.Equals(n, binderName, StringComparison.OrdinalIgnoreCase)) { memAnchor = b; break; }
        }
        if (memAnchor == null) return Array.Empty<Guid>();

        return db.Core.CardsIn(memAnchor.Value).Select(c => c.Value).ToList();
    }
}
