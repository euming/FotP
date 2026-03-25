using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CardBinder.Core;

namespace MemoryCtl;

internal static partial class Commands
{
    public static int Delta(string dbPath, string channel, string chatId, string q, int top, int maxChars, string? tailPath, int tailMaxChars)
    {
        var text = DeltaContext.Build(dbPath, new DeltaContext.Options(
            Channel: channel,
            ChatId: chatId,
            Query: q,
            Top: top,
            MaxChars: maxChars,
            TranscriptTailPath: tailPath,
            TailMaxChars: tailMaxChars));

        if (!string.IsNullOrWhiteSpace(text))
            Console.WriteLine(text);

        return 0;
    }

    public static int RenderBinder(string dbPath, string binderName, int maxChars, bool includeIds)
    {
        var text = BinderRenderer.Render(dbPath, new BinderRenderer.RenderOptions(
            BinderName: binderName,
            MaxChars: maxChars,
            IncludeIds: includeIds));

        if (!string.IsNullOrWhiteSpace(text))
            Console.WriteLine(text);

        return 0;
    }

    public static int MemAnchorPage(string dbPath, string memAnchorName, string? outPath)
    {
        var db = MemoryJsonlReader.Load(dbPath);

        Func<MemAnchorId, string?> binderName = b => db.Core.TryGetBinderName(b, out var n) ? n : null;

        // Resolve memAnchor by exact (case-insensitive) name.
        var target = db.Core.AllBinders.FirstOrDefault(b =>
        {
            var name = binderName(b);
            return !string.IsNullOrWhiteSpace(name) &&
                   string.Equals(name, memAnchorName, StringComparison.OrdinalIgnoreCase);
        });

        if (target.Value == Guid.Empty)
            throw new ArgumentException($"MemAnchor not found: '{memAnchorName}'");

        var resolvedName = binderName(target) ?? memAnchorName;
        var cards = db.Core.CardsIn(target).ToList();

        // Choose a simple summary card: first card with payload text.
        CardId? summaryCard = null;
        foreach (var c in cards)
        {
            if (db.TryGetPayload(c, out var payload) && payload?.Text is { Length: > 0 })
            {
                summaryCard = c;
                break;
            }
        }

        string? summaryText = null;
        if (summaryCard is { } sc && db.TryGetPayload(sc, out var payloadSummary) && payloadSummary is not null)
        {
            var title = payloadSummary.Title ?? sc.Value.ToString();
            var text = payloadSummary.Text ?? string.Empty;
            var (_, _, desc) = ParseRoadmapFields(text, title);
            summaryText = OneLine(desc);
        }

        var nowUtc = DateTimeOffset.UtcNow;

        using var sw = new StringWriter();
        sw.WriteLine("<!DOCTYPE html>");
        sw.WriteLine("<html>");
        sw.WriteLine("<head>");
        sw.WriteLine("  <meta charset=\"utf-8\" />");
        sw.WriteLine($"  <title>{System.Net.WebUtility.HtmlEncode(resolvedName)}</title>");
        sw.WriteLine($"  <meta name=\"memanchor-id\" content=\"{target.Value}\" />");
        sw.WriteLine($"  <meta name=\"memanchor-name\" content=\"{System.Net.WebUtility.HtmlEncode(resolvedName)}\" />");
        sw.WriteLine("</head>");
        sw.WriteLine("<body>");
        sw.WriteLine($"  <h1>{System.Net.WebUtility.HtmlEncode(resolvedName)}</h1>");
        sw.WriteLine($"  <p>Generated at: {nowUtc:O} (UTC)</p>");

        if (!string.IsNullOrWhiteSpace(summaryText))
        {
            sw.WriteLine("  <p>" + System.Net.WebUtility.HtmlEncode(summaryText) + "</p>");
        }

        sw.WriteLine("  <ul>");
        foreach (var card in cards)
        {
            if (!db.TryGetPayload(card, out var payload) || payload is null)
                continue;

            var title = payload.Title ?? card.Value.ToString();
            var text = payload.Text ?? string.Empty;
            var (status, area, desc) = ParseRoadmapFields(text, title);

            var titleHtml = System.Net.WebUtility.HtmlEncode(title);
            var statusHtml = System.Net.WebUtility.HtmlEncode(status);
            var areaHtml = System.Net.WebUtility.HtmlEncode(area);
            var descHtml = System.Net.WebUtility.HtmlEncode(OneLine(desc));

            // Collect memAnchors (binders) for this card.
            var memAnchors = db.Core.BindersOf(card)
                .Select(b => binderName(b))
                .Where(n => !string.IsNullOrWhiteSpace(n))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
                .ToList();

            sw.Write("    <li><strong>");
            sw.Write(titleHtml);
            sw.Write("</strong> — Status: ");
            sw.Write(statusHtml);
            sw.Write(", Area: ");
            sw.Write(areaHtml);
            sw.Write(" – ");
            sw.Write(descHtml);

            if (memAnchors.Count > 0)
            {
                sw.Write("<br/>MemAnchors: ");
                for (int i = 0; i < memAnchors.Count; i++)
                {
                    var name = memAnchors[i]!;
                    var safe = MakeSafeFileName(name);
                    var nameHtml = System.Net.WebUtility.HtmlEncode(name);
                    var href = $"../memanchor_pages/{safe}.html";

                    sw.Write("<a href=\"");
                    sw.Write(System.Net.WebUtility.HtmlEncode(href));
                    sw.Write("\">");
                    sw.Write(nameHtml);
                    sw.Write("</a>");

                    if (i < memAnchors.Count - 1)
                        sw.Write(", ");
                }
            }

            sw.WriteLine("</li>");
        }

        sw.WriteLine("  </ul>");
        sw.WriteLine("</body>");
        sw.WriteLine("</html>");

        var html = sw.ToString();

        // Compute default output path if needed.
        if (string.IsNullOrWhiteSpace(outPath))
        {
            var dbDir = Path.GetDirectoryName(Path.GetFullPath(dbPath)) ?? Directory.GetCurrentDirectory();
            var fileName = MakeSafeFileName(memAnchorName) + ".html";

            string targetDir;
            var dbFile = Path.GetFileName(dbPath);
            if (dbFile.Equals("memory.jsonl", StringComparison.OrdinalIgnoreCase))
            {
                // ...\memory\memory_graph\memory.jsonl => ...\memory\memanchor_pages
                targetDir = Path.GetFullPath(Path.Combine(dbDir, "..", "..", "memanchor_pages"));
            }
            else
            {
                targetDir = Path.Combine(dbDir, "memanchor_pages");
            }

            outPath = Path.Combine(targetDir, fileName);
        }

        var outDir = Path.GetDirectoryName(outPath);
        if (!string.IsNullOrWhiteSpace(outDir) && !Directory.Exists(outDir))
            Directory.CreateDirectory(outDir);

        File.WriteAllText(outPath, html);
        Console.WriteLine(outPath);

        return 0;
    }

    public static int BuildInjectBinder(string dbPath, string channel, string chatId, string chatLabel, string q, int top, int maxLinks, bool perRun)
    {
        var binderName = InjectBinderBuilder.Build(
            dbPath: dbPath,
            deltaOpts: new DeltaContext.Options(channel, chatId, q, top, MaxChars: 0, TranscriptTailPath: null, TailMaxChars: 0),
            opts: new InjectBinderBuilder.BuildOptions(
                Channel: channel,
                ChatId: chatId,
                ChatLabel: chatLabel,
                Query: q,
                Top: top,
                MaxLinks: maxLinks,
                Relevance: 0.8f,
                Reason: "inject-memAnchor build",
                PerRunBinder: perRun));

        if (!string.IsNullOrWhiteSpace(binderName))
            Console.WriteLine(binderName);

        return 0;
    }

    public static int LogInjection(string dbPath, string channel, string chatId, string chatLabel, string binderName, string ledgerPath, int maxChars, string reason)
    {
        var rendered = BinderRenderer.Render(dbPath, new BinderRenderer.RenderOptions(binderName, maxChars, IncludeIds: true));
        if (string.IsNullOrWhiteSpace(rendered))
            return 0;

        var db = MemoryJsonlReader.Load(dbPath);
        var cardIds = InjectionLedger.CollectCardIdsForBinder(db, binderName);

        var entry = new InjectionLedger.Entry(
            Ts: DateTimeOffset.Now,
            Channel: channel,
            ChatId: chatId,
            ChatLabel: chatLabel,
            BinderName: binderName,
            CardIds: cardIds,
            RenderedChars: rendered.Length,
            RenderedSha256: InjectionLedger.Sha256Hex(rendered),
            Reason: reason);

        InjectionLedger.Append(ledgerPath, entry);
        return 0;
    }

    public static int InjectPlan(
        string dbPath,
        string channel,
        string chatId,
        string chatLabel,
        string q,
        int top,
        int maxLinks,
        bool perRun,
        int maxChars,
        string ledgerPath,
        string reason)
    {
        var binderName = InjectBinderBuilder.Build(
            dbPath: dbPath,
            deltaOpts: new DeltaContext.Options(channel, chatId, q, top, MaxChars: 0, TranscriptTailPath: null, TailMaxChars: 0),
            opts: new InjectBinderBuilder.BuildOptions(
                Channel: channel,
                ChatId: chatId,
                ChatLabel: chatLabel,
                Query: q,
                Top: top,
                MaxLinks: maxLinks,
                Relevance: 0.8f,
                Reason: reason,
                PerRunBinder: perRun));

        if (string.IsNullOrWhiteSpace(binderName))
            return 0;

        var rendered = BinderRenderer.Render(dbPath, new BinderRenderer.RenderOptions(binderName, maxChars, IncludeIds: true));
        if (string.IsNullOrWhiteSpace(rendered))
            return 0;

        // Write ledger
        LogInjection(dbPath, channel, chatId, chatLabel, binderName, ledgerPath, maxChars, reason);

        // Emit the injection block (so callers can prepend it to an LLM call)
        Console.WriteLine(rendered);
        return 0;
    }
}
