using System;
using System.Linq;
using MemoryGraph.Application;

namespace MemoryCtl;

public static class Program
{
    private static readonly HashSet<string> ShadowCompareSafeCommands = new(StringComparer.OrdinalIgnoreCase)
    {
        "validate",
        "query",
        "prompt",
        "suggest-memAnchors",
        "list-memanchors",
        "render-memAnchor",
        "delta",
        "build-inject"
    };

    private static void PrintHelp()
    {
        Console.WriteLine("memoryctl - portable memory query tool\n");
        Console.WriteLine("Global options:");
        Console.WriteLine("  --backend <legacy|ams>   Select graph backend (default: ams; legacy is deprecated rollback-only)");
        Console.WriteLine("  --shadow-compare         Run safe read-only commands on legacy+ams and report deterministic diffs");
        Console.WriteLine();
        Console.WriteLine("Commands:");
        Console.WriteLine("  validate        --db <path>");
        Console.WriteLine("  export-graph    --db <path> --out <path>");
        Console.WriteLine("  query           --db <path> --q <query> [--top N] [--memAnchor <name>] [--explain]");
        Console.WriteLine("  prompt          --db <path> [--q <query>] [--top N] [--binder <name>[,<name>...]] [--memAnchor <name>[,<name>...]] [--seed-container <id>[,<id>...]] [--seed-object <id>[,<id>...]] [--max-objects N] [--ordering ordinal|seed-first]");
        Console.WriteLine("  add             --db <path> --title <text> --text <text> [--memAnchor <name>]... [--source <text>] [--key <stable-key>]");
        Console.WriteLine("  suggest-memAnchors --db <path> --q <query> [--top N]");
        Console.WriteLine("  list-memanchors --db <path>");
        Console.WriteLine("  maintain        --db <path> --card <guid> [--top N] [--apply] [--reason <text>] [--relevance <0..1>]");
        Console.WriteLine("  ingest-chatlog  --db <path> --chatlog <path> --cursor <path> [--max N] [--gap-min N] [--dream] [--raw-llm <path>]");
        Console.WriteLine("  append-chat-event --chatlog <path> --channel <name> --chat-id <id> --message-id <id> --direction in|out --text <text> [--author <text>] [--ts <rfc3339>]");
        Console.WriteLine("  build-transcript --raw-user <path> --raw-llm <path> --out <path> [--md <path>] [--html <path>] --channel <name> --chat-id <id>");
        Console.WriteLine("  sync-rawllm --sessions <sessions.json> --raw-llm-dir <dir> --cursor-dir <dir>");
        Console.WriteLine("  sync-rawuser --sessions <sessions.json> --raw-user-dir <dir> --cursor-dir <dir>");
        Console.WriteLine("  ingest-systemlogs --db <path> --log-dir <dir> --cursor-dir <dir> [--max N]");
        Console.WriteLine("  delta --db <path> --channel <name> --chat <label-or-id> --q <text> [--registry <path>] [--top N] [--max-chars N] [--tail <path>] [--tail-max-chars N]");
        Console.WriteLine("  render-memAnchor --db <path> --memAnchor <exact-name> [--max-chars N] [--ids]");
        Console.WriteLine("  memanchor-page  --db <path> --memAnchor <name> [--out <path>]");
        Console.WriteLine("  build-inject --db <path> --channel <name> --chat <label-or-id> [--registry <path>] --q <text> [--top N] [--max-links N] [--per-run]");
        Console.WriteLine("  log-injection --db <path> --channel <name> --chat <label-or-id> [--registry <path>] --memAnchor <exact-name> [--ledger <path>] [--max-chars N] [--reason <text>]");
        Console.WriteLine("  inject-plan  --db <path> --channel <name> --chat <label-or-id> [--registry <path>] --q <text> [--top N] [--max-links N] [--per-run] [--max-chars N] [--ledger <path>] [--reason <text>]");
        Console.WriteLine("  make-memAnchor  --db <path> --name <memAnchor> --q <text> [--top N] [--memAnchor <filter>] [--relevance <0..1>] [--reason <text>]");
        Console.WriteLine("  build-transcript-clean --raw-user <path> --raw-llm <path> --out <path> [--md <path>] [--html <path>] --channel <name> --chat-id <id> --db <path> --deleted <path>");
        Console.WriteLine("  debug-ams       --db <path> [--out <path>] [--open-anchor <anchor>]");
        Console.WriteLine("  atlas-page      --db <path> --page-id <id>");
        Console.WriteLine("  atlas-search    --db <path> --q <query> [--top N]");
        Console.WriteLine("  atlas-expand    --db <path> --ref-id <id>");
        Console.WriteLine("  list-sessions   --db <path> [--since YYYY-MM-DD] [--n N]");
        Console.WriteLine("  show-session    --db <path> --id <guid-or-prefix>");
        Console.WriteLine("  show-conv       --db <path> --id <guid>");
        Console.WriteLine("  show-seg        --db <path> --id <guid>:<n>");
        Console.WriteLine("  show-turn       --db <path> --id <guid>:<t>");
        Console.WriteLine("  dream           --db <path> [--topic-k N] [--thread-k N] [--decision-k N] [--invariant-k N] [--dry-run]");
        Console.WriteLine("  dream-relax     --db <path> [--steps N] [--accepted N] [--temperature T] [--seed N] [--dry-run]");
        Console.WriteLine("  agent-maintain  --db <path>");
        Console.WriteLine("  retrieval-graph-materialize --db <path>");
        Console.WriteLine("  agent-query     --db <path> --q <query> [--top N] [--explain] [--record-route] [--current-node <id>] [--parent-node <id>] [--grandparent-node <id>] [--role <name>] [--mode <name>] [--failure-bucket <name>] [--artifact <path>[,<path>...]] [--traversal-budget N] [--no-active-thread-context] [--bias-scale <0..1>] [--min-strong-wins N] [--min-bias <0..1>]");
        Console.WriteLine("  smartlist-create --db <path> --path <smartlist/path> [--durable] [--created-by <text>]");
        Console.WriteLine("  smartlist-note  --db <path> --title <text> --text <text> [--in <smartlist/path>[,<smartlist/path>...]] [--durable] [--created-by <text>]");
        Console.WriteLine("  smartlist-attach --db <path> --path <smartlist/path> --member <smartlist/path|object-id> [--created-by <text>]");
        Console.WriteLine("  smartlist-inspect --db <path> --path <smartlist/path> [--depth N]");
        Console.WriteLine("  smartlist-remember --db <path> [--path <smartlist/path> | --id <object-id>]");
        Console.WriteLine("  smartlist-visibility --db <path> --path <smartlist/path> --visibility <default|scoped|suppressed> [--recursive] [--include-notes] [--include-rollups]");
        Console.WriteLine("  smartlist-rollup --db <path> --path <smartlist/path> --summary <text> --scope <text> [--stop-hint <text>] [--child <smartlist/path>::<summary>[,<smartlist/path>::<summary>...]] [--durable] [--created-by <text>]");
        Console.WriteLine("  smartlist-rollup-show --db <path> --path <smartlist/path>");
        Console.WriteLine("  bugreport-create --db <path> --source-agent <name> --parent-agent <name> --error-output <text> --stack-context <text> --severity <critical|high|medium|low> [--attempted-fix <text>[,...]] [--reproduction-step <text>[,...]] [--recommended-fix-plan <text>] [--durable] [--created-by <text>]");
        Console.WriteLine("  bugreport-update-status --db <path> --bug-id <id> --status <open|in-repair|resolved>");
        Console.WriteLine("  bugreport-show  --db <path> --bug-id <id>");
        Console.WriteLine("  bugreport-list  --db <path> [--status <open|in-repair|resolved>]");
        Console.WriteLine("  bugreport-search --db <path> --query <text> [--status <open|in-repair|resolved>]");
        Console.WriteLine("  bugfix-create   --db <path> --title <text> --description <text> --fix-recipe <text> [--linked-bugreport <id>] [--durable] [--created-by <text>]");
        Console.WriteLine("  bugfix-show     --db <path> --fix-id <id>");
        Console.WriteLine("  bugfix-list     --db <path>");
        Console.WriteLine("  bugfix-link     --db <path> --bug-id <bugreport-id> --fix-id <bugfix-id> [--created-by <text>]");
        Console.WriteLine("  thread-status   --db <path>");
        Console.WriteLine("  thread-start    --db <path> [--id <thread-id>] --title <text> --current-step <text> --next-command <text> [--branch-off-anchor <text>] [--artifact-ref <text>]");
        Console.WriteLine("  thread-push-tangent --db <path> [--id <thread-id>] --title <text> --current-step <text> --next-command <text> [--branch-off-anchor <text>] [--artifact-ref <text>]");
        Console.WriteLine("  thread-checkpoint --db <path> --current-step <text> --next-command <text> [--branch-off-anchor <text>] [--artifact-ref <text>]");
        Console.WriteLine("  thread-archive  --db <path> [--id <thread-id>]");
        Console.WriteLine("  thread-pop      --db <path>");
        Console.WriteLine("  thread-list     --db <path>");
        Console.WriteLine("  agent-capability-upsert --db <path> --agent <name> --capability-key <key> --state <mirrored|partial|workaround|missing|intentional_asymmetry> --problem-key <key> --equivalence-group-key <key> [--summary <text>] [--notes <text>] [--created-by <text>]");
        Console.WriteLine("  agent-capability-show --db <path> [--id <entry-id> | --agent <name> --capability-key <key>]");
        Console.WriteLine("  agent-capability-list --db <path> [--agent <name> | --problem <key> | --group <key>]");
        Console.WriteLine("  relay-run       --db <path> --task-thread-id <id> [--inject-handoff-failure <mode>]");
        Console.WriteLine("  route-replay    --db <path> --input <replay.jsonl> --out <results.jsonl> [--top N]");
    }

    public static int Main(string[] args)
    {
        try
        {
            var p = ArgParser.Parse(args);
            if (string.IsNullOrWhiteSpace(p.Command))
            {
                PrintHelp();
                return 1;
            }

            if (p.HasFlag("shadow-compare"))
                return RunShadowCompare(args, p.Command!);

            var composition = MemoryCtlCompositionRoot.Build(ResolveBackendOption(p));
            return ExecuteCommand(p, composition);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 2;
        }
    }

    private static int RunShadowCompare(string[] args, string command)
    {
        if (!ShadowCompareSafeCommands.Contains(command))
            throw new ArgumentException($"--shadow-compare is supported only for safe read-only commands: {string.Join(", ", ShadowCompareSafeCommands.OrderBy(x => x, StringComparer.Ordinal))}");

        var legacyArgs = BuildBackendArgs(args, "legacy");
        var amsArgs = BuildBackendArgs(args, "ams");

        var legacyResult = RunCaptured(legacyArgs);
        Console.Write(legacyResult.Stdout);
        Console.Error.Write(legacyResult.Stderr);

        if (legacyResult.ExitCode != 0)
        {
            Console.Error.WriteLine("[shadow-compare] skipped AMS compare because legacy execution failed.");
            return legacyResult.ExitCode;
        }

        var amsResult = RunCaptured(amsArgs);
        var report = ShadowCompareReporter.BuildReport(command, legacyResult, amsResult);
        Console.Error.WriteLine(report.Message);
        return report.ExitCode;
    }

    private static string[] BuildBackendArgs(string[] args, string backend)
    {
        var list = new List<string>();
        for (int i = 0; i < args.Length; i++)
        {
            var current = args[i];
            if (string.Equals(current, "--shadow-compare", StringComparison.OrdinalIgnoreCase))
                continue;

            if (string.Equals(current, "--backend", StringComparison.OrdinalIgnoreCase))
            {
                i++;
                continue;
            }

            list.Add(current);
        }

        list.Add("--backend");
        list.Add(backend);
        return list.ToArray();
    }

    private static CommandRunResult RunCaptured(string[] args)
    {
        var oldOut = Console.Out;
        var oldErr = Console.Error;

        using var outWriter = new StringWriter();
        using var errWriter = new StringWriter();

        Console.SetOut(outWriter);
        Console.SetError(errWriter);

        try
        {
            var parser = ArgParser.Parse(args);
            var composition = MemoryCtlCompositionRoot.Build(ResolveBackendOption(parser));
            var code = ExecuteCommand(parser, composition);
            return new CommandRunResult(code, outWriter.ToString(), errWriter.ToString());
        }
        finally
        {
            Console.SetOut(oldOut);
            Console.SetError(oldErr);
        }
    }

    private static string? ResolveBackendOption(ArgParser parser)
    {
        var backend = parser.GetOptional("backend");
        if (parser.HasOption("backend") && string.IsNullOrWhiteSpace(backend))
            throw new ArgumentException("Missing value for --backend. Supported values: legacy|ams.");

        return backend;
    }

    private static int ExecuteCommand(ArgParser p, MemoryCtlComposition composition)
    {
        var cmd = p.Command!.ToLowerInvariant();
        switch (cmd)
        {
            case "validate":
            {
                var db = p.GetRequired("db");
                return Commands.Validate(db);
            }

            case "export-graph":
            {
                var db = p.GetRequired("db");
                var outPath = p.GetRequired("out");
                return Commands.ExportGraph(db, outPath);
            }

            case "query":
            {
                var db = p.GetRequired("db");
                var q = p.GetRequired("q");
                var top = p.GetInt("top", 10);
                var memAnchor = p.GetOptional("memAnchor");
                var explain = p.HasFlag("explain");
                return composition.GraphCommands.Query(db, q, top, memAnchor, explain);
            }

            case "prompt":
            {
                var db = p.GetRequired("db");
                var top = p.GetInt("top", 20);

                static List<string> ParseMulti(string? raw)
                {
                    if (string.IsNullOrWhiteSpace(raw)) return new List<string>();
                    var parts = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                    return new List<string>(parts);
                }

                var binderRaw = p.GetOptional("binder");
                var memAnchorRaw = p.GetOptional("memAnchor");
                var binderNames = ParseMulti(binderRaw);
                binderNames.AddRange(ParseMulti(memAnchorRaw));

                var q = p.GetOptional("q");
                if (string.IsNullOrWhiteSpace(q))
                {
                    if (binderNames.Count > 0)
                        q = string.Join(' ', binderNames);
                    else
                        throw new ArgumentException("Missing required option --q (or at least one --binder/--memAnchor).");
                }

                var seedContainers = ParseMulti(p.GetOptional("seed-container"));
                var seedObjects = ParseMulti(p.GetOptional("seed-object"));
                var maxObjects = p.GetInt("max-objects", top);
                var orderingRaw = p.GetOptional("ordering") ?? "ordinal";
                var ordering = orderingRaw.Equals("seed-first", StringComparison.OrdinalIgnoreCase)
                    ? AMS.Core.ContextObjectOrdering.SeedFirst
                    : AMS.Core.ContextObjectOrdering.Ordinal;

                return composition.GraphCommands.Prompt(db, q!, top, binderNames, seedContainers, seedObjects, maxObjects, ordering);
            }

            case "add":
            {
                var db = p.GetRequired("db");
                var title = p.GetRequired("title");
                var text = p.GetRequired("text");
                var source = p.GetOptional("source");
                var key = p.GetOptional("key");

                var binderRaw = p.GetOptional("memAnchor");
                var memAnchors = string.IsNullOrWhiteSpace(binderRaw)
                    ? Array.Empty<string>()
                    : binderRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                return composition.GraphCommands.Add(db, title, text, memAnchors, source, key);
            }

            case "suggest-memAnchors":
            {
                var db = p.GetRequired("db");
                var q = p.GetRequired("q");
                var top = p.GetInt("top", 10);
                return Commands.SuggestBinders(db, q, top);
            }

            case "list-memanchors":
            {
                var db = p.GetRequired("db");
                return Commands.ListMemAnchors(db);
            }

            case "maintain":
            {
                var db = p.GetRequired("db");
                var cardStr = p.GetRequired("card");
                if (!Guid.TryParse(cardStr, out var cardId))
                    throw new ArgumentException($"Invalid --card GUID: '{cardStr}'");

                var top = p.GetInt("top", 10);
                var apply = p.HasFlag("apply");
                var reason = p.GetOptional("reason") ?? "robots dreaming of electric sheep";

                float relevance = 0.75f;
                var relStr = p.GetOptional("relevance");
                if (!string.IsNullOrWhiteSpace(relStr))
                {
                    if (!float.TryParse(relStr, out relevance))
                        throw new ArgumentException($"Invalid --relevance float: '{relStr}'");
                    relevance = Math.Clamp(relevance, 0f, 1f);
                }

                return composition.GraphCommands.Maintain(db, cardId, top, apply, reason, relevance);
            }

            case "ingest-chatlog":
            {
                var db = p.GetRequired("db");
                var chatlog = p.GetRequired("chatlog");
                var cursor = p.GetRequired("cursor");
                var max = p.GetInt("max", 200);
                var gapMin = p.GetInt("gap-min", 10);
                var dream = p.HasFlag("dream");
                var rawLlm = p.GetOptional("raw-llm");
                return composition.IngestSyncCommands.IngestChatlog(db, chatlog, cursor, max, gapMin, dream, rawLlm);
            }

            case "append-chat-event":
            {
                var chatlog = p.GetRequired("chatlog");
                var channel = p.GetRequired("channel");
                var chatId = p.GetRequired("chat-id");
                var msgId = p.GetRequired("message-id");
                var direction = p.GetRequired("direction");
                var text = p.GetRequired("text");
                var author = p.GetOptional("author");

                DateTimeOffset ts = DateTimeOffset.Now;
                var tsStr = p.GetOptional("ts");
                if (!string.IsNullOrWhiteSpace(tsStr))
                {
                    if (!DateTimeOffset.TryParse(tsStr, out ts))
                        throw new ArgumentException($"Invalid --ts DateTimeOffset: '{tsStr}'");
                }

                return Commands.AppendChatEvent(chatlog, channel, chatId, msgId, ts, author, direction, text);
            }

            case "build-transcript":
            {
                var rawUser = p.GetRequired("raw-user");
                var rawLlm = p.GetRequired("raw-llm");
                var outPath = p.GetRequired("out");
                var md = p.GetOptional("md");
                var html = p.GetOptional("html");
                var channel = p.GetRequired("channel");
                var chatId = p.GetRequired("chat-id");

                return Commands.BuildTranscript(rawUser, rawLlm, outPath, md, html, channel, chatId);
            }

            case "build-transcript-clean":
            {
                var rawUser = p.GetRequired("raw-user");
                var rawLlm = p.GetRequired("raw-llm");
                var outPath = p.GetRequired("out");
                var md = p.GetOptional("md");
                var html = p.GetOptional("html");
                var channel = p.GetRequired("channel");
                var chatId = p.GetRequired("chat-id");
                var db = p.GetRequired("db");
                var deleted = p.GetRequired("deleted");

                return Commands.BuildTranscriptClean(rawUser, rawLlm, outPath, md, html, channel, chatId, db, deleted);
            }

            case "sync-rawllm":
            {
                var sessions = p.GetRequired("sessions");
                var rawLlmDir = p.GetRequired("raw-llm-dir");
                var cursorDir = p.GetRequired("cursor-dir");
                return composition.IngestSyncCommands.SyncRawLlmFromSessions(sessions, rawLlmDir, cursorDir);
            }

            case "sync-rawuser":
            {
                var sessions = p.GetRequired("sessions");
                var rawUserDir = p.GetRequired("raw-user-dir");
                var cursorDir = p.GetRequired("cursor-dir");
                return composition.IngestSyncCommands.SyncRawUserFromSessions(sessions, rawUserDir, cursorDir);
            }

            case "ingest-systemlogs":
            {
                var db = p.GetRequired("db");
                var logDir = p.GetRequired("log-dir");
                var cursorDir = p.GetRequired("cursor-dir");
                var max = p.GetInt("max", 200);
                return composition.IngestSyncCommands.IngestSystemLogs(db, logDir, cursorDir, max);
            }

            case "delta":
            {
                var db = p.GetRequired("db");
                var channel = p.GetRequired("channel");
                var chat = p.GetRequired("chat");
                var q = p.GetRequired("q");

                var registryPath = p.GetOptional("registry")
                    ?? "C:\\Users\\eumin\\.openclaw\\workspace\\memory\\memory_graph\\registry\\chats.json";

                var entries = File.Exists(registryPath) ? ChatRegistry.Load(registryPath) : Array.Empty<ChatRegistry.Entry>();
                if (!ChatRegistry.TryResolveChatId(entries, channel, chat, out var chatId))
                    throw new ArgumentException($"Unable to resolve --chat '{chat}' for channel '{channel}'.");

                var top = p.GetInt("top", 5);
                var maxChars = p.GetInt("max-chars", 1200);
                var tail = p.GetOptional("tail");
                var tailMaxChars = p.GetInt("tail-max-chars", 8000);

                return Commands.Delta(db, channel, chatId, q, top, maxChars, tail, tailMaxChars);
            }

            case "render-memAnchor":
            {
                var db = p.GetRequired("db");
                var memAnchor = p.GetRequired("memAnchor");
                var maxChars = p.GetInt("max-chars", 1200);
                var ids = p.HasFlag("ids");
                return Commands.RenderBinder(db, memAnchor, maxChars, ids);
            }

            case "memanchor-page":
            {
                var db = p.GetRequired("db");
                var memAnchor = p.GetRequired("memAnchor");
                var outPath = p.GetOptional("out");
                return Commands.MemAnchorPage(db, memAnchor, outPath);
            }

            case "build-inject":
            {
                var db = p.GetRequired("db");
                var channel = p.GetRequired("channel");
                var chat = p.GetRequired("chat");
                var q = p.GetRequired("q");

                var registryPath = p.GetOptional("registry")
                    ?? "C:\\Users\\eumin\\.openclaw\\workspace\\memory\\memory_graph\\registry\\chats.json";

                var entries = File.Exists(registryPath) ? ChatRegistry.Load(registryPath) : Array.Empty<ChatRegistry.Entry>();
                if (!ChatRegistry.TryResolveChatId(entries, channel, chat, out var chatId))
                    throw new ArgumentException($"Unable to resolve --chat '{chat}' for channel '{channel}'.");

                var chatLabel = chat;
                var top = p.GetInt("top", 5);
                var maxLinks = p.GetInt("max-links", top);
                var perRun = p.HasFlag("per-run");

                return Commands.BuildInjectBinder(db, channel, chatId, chatLabel, q, top, maxLinks, perRun);
            }

            case "log-injection":
            {
                var db = p.GetRequired("db");
                var channel = p.GetRequired("channel");
                var chat = p.GetRequired("chat");
                var memAnchor = p.GetRequired("memAnchor");

                var registryPath = p.GetOptional("registry")
                    ?? "C:\\Users\\eumin\\.openclaw\\workspace\\memory\\memory_graph\\registry\\chats.json";

                var entries = File.Exists(registryPath) ? ChatRegistry.Load(registryPath) : Array.Empty<ChatRegistry.Entry>();
                if (!ChatRegistry.TryResolveChatId(entries, channel, chat, out var chatId))
                    throw new ArgumentException($"Unable to resolve --chat '{chat}' for channel '{channel}'.");

                var chatLabel = chat;
                var ledger = p.GetOptional("ledger")
                    ?? "C:\\Users\\eumin\\.openclaw\\workspace\\memory\\memory_graph\\rawSystem\\injections\\telegram\\injections.jsonl";

                var maxChars = p.GetInt("max-chars", 1200);
                var reason = p.GetOptional("reason") ?? "manual";

                return Commands.LogInjection(db, channel, chatId, chatLabel, memAnchor, ledger, maxChars, reason);
            }

            case "inject-plan":
            {
                var db = p.GetRequired("db");
                var channel = p.GetRequired("channel");
                var chat = p.GetRequired("chat");
                var q = p.GetRequired("q");

                var registryPath = p.GetOptional("registry")
                    ?? "C:\\Users\\eumin\\.openclaw\\workspace\\memory\\memory_graph\\registry\\chats.json";

                var entries = File.Exists(registryPath) ? ChatRegistry.Load(registryPath) : Array.Empty<ChatRegistry.Entry>();
                if (!ChatRegistry.TryResolveChatId(entries, channel, chat, out var chatId))
                    throw new ArgumentException($"Unable to resolve --chat '{chat}' for channel '{channel}'.");

                var chatLabel = chat;
                var top = p.GetInt("top", 5);
                var maxLinks = p.GetInt("max-links", top);
                var perRun = p.HasFlag("per-run");

                var maxChars = p.GetInt("max-chars", 1200);
                var ledger = p.GetOptional("ledger")
                    ?? "C:\\Users\\eumin\\.openclaw\\workspace\\memory\\memory_graph\\rawSystem\\injections\\telegram\\injections.jsonl";
                var reason = p.GetOptional("reason") ?? "auto";

                return Commands.InjectPlan(db, channel, chatId, chatLabel, q, top, maxLinks, perRun, maxChars, ledger, reason);
            }

            case "make-memAnchor":
            {
                var db = p.GetRequired("db");
                var name = p.GetRequired("name");
                var q = p.GetRequired("q");
                var top = p.GetInt("top", 20);
                var memAnchor = p.GetOptional("memAnchor");
                var reason = p.GetOptional("reason") ?? "make-memAnchor";
                float relevance = 0.8f;
                var relStr = p.GetOptional("relevance");
                if (!string.IsNullOrWhiteSpace(relStr))
                {
                    if (!float.TryParse(relStr, out relevance))
                        throw new ArgumentException($"Invalid --relevance float: '{relStr}'");
                    relevance = Math.Clamp(relevance, 0f, 1f);
                }

                return composition.GraphCommands.MakeMemAnchor(db, name, q, top, memAnchor, relevance, reason);
            }

            case "debug-ams":
            {
                var db = p.GetRequired("db");
                var outPath = p.GetOptional("out");
                var openAnchor = p.GetOptional("open-anchor");
                return composition.GraphCommands.DebugAms(db, outPath, openAnchor);
            }

            case "atlas-page":
            {
                var db = p.GetRequired("db");
                var pageId = p.GetRequired("page-id");
                return composition.GraphCommands.AtlasPage(db, pageId);
            }

            case "atlas-search":
            {
                var db  = p.GetRequired("db");
                var q   = p.GetRequired("q");
                var top = p.GetInt("top", 20);
                return composition.GraphCommands.AtlasSearch(db, q, top);
            }

            case "atlas-expand":
            {
                var db    = p.GetRequired("db");
                var refId = p.GetRequired("ref-id");
                return composition.GraphCommands.AtlasExpand(db, refId);
            }

            case "list-sessions":
            {
                var db    = p.GetRequired("db");
                var since = p.GetOptional("since");
                var n     = p.GetInt("n", 20);
                return composition.GraphCommands.ListSessions(db, since, n);
            }

            case "show-session":
            {
                var db   = p.GetRequired("db");
                var id   = p.GetRequired("id");
                var html = p.GetOptional("html");
                return composition.GraphCommands.ShowSession(db, id, html);
            }

            case "show-conv":
            {
                var db = p.GetRequired("db");
                var id = p.GetRequired("id");
                return composition.GraphCommands.AtlasPage(db, $"chat-session:{id}");
            }

            case "show-seg":
            {
                var db = p.GetRequired("db");
                var id = p.GetRequired("id");
                return composition.GraphCommands.AtlasPage(db, $"seg:{id}");
            }

            case "show-turn":
            {
                var db = p.GetRequired("db");
                var id = p.GetRequired("id");
                return composition.GraphCommands.AtlasPage(db, $"turn:{id}");
            }

            case "dream":
            {
                var db         = p.GetRequired("db");
                var topicK     = p.GetInt("topic-k",    5);
                var threadK    = p.GetInt("thread-k",   3);
                var decisionK  = p.GetInt("decision-k", 3);
                var invariantK = p.GetInt("invariant-k",3);
                var dryRun     = p.HasFlag("dry-run");
                return composition.GraphCommands.Dream(db, topicK, threadK, decisionK, invariantK, dryRun);
            }

            case "dream-relax":
            {
                var db          = p.GetRequired("db");
                var maxSteps    = p.GetInt("steps",       500);
                var maxAccepted = p.GetInt("accepted",    100);
                var temperature = double.TryParse(p.GetOptional("temperature"), out var t) ? t : 0.0;
                var seed        = p.GetInt("seed",        42);
                var dryRun      = p.HasFlag("dry-run");
                return composition.GraphCommands.DreamRelax(db, maxSteps, maxAccepted, temperature, seed, dryRun);
            }

            case "agent-maintain":
            {
                var db = p.GetRequired("db");
                return composition.GraphCommands.AgentMaintain(db);
            }

            case "retrieval-graph-materialize":
            {
                var db = p.GetRequired("db");
                return composition.GraphCommands.RetrievalGraphMaterialize(db);
            }

            case "agent-query":
            {
                var db = p.GetRequired("db");
                var q = p.GetRequired("q");
                var top = p.GetInt("top", 8);
                var explain = p.HasFlag("explain");
                var recordRoute = p.HasFlag("record-route");
                var currentNode = p.GetOptional("current-node");
                var parentNode = p.GetOptional("parent-node");
                var grandparentNode = p.GetOptional("grandparent-node");
                var role = p.GetOptional("role");
                var mode = p.GetOptional("mode");
                var failureBucket = p.GetOptional("failure-bucket");
                var artifactRaw = p.GetOptional("artifact");
                var artifacts = string.IsNullOrWhiteSpace(artifactRaw)
                    ? Array.Empty<string>()
                    : artifactRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                var traversalBudget = p.GetInt("traversal-budget", 3);
                var noActiveThreadContext = p.HasFlag("no-active-thread-context");
                var biasScale = p.GetDouble("bias-scale", 1.0d);
                var minStrongWins = p.GetInt("min-strong-wins", 1);
                var minBias = p.GetDouble("min-bias", 0.0001d);
                RouteMemoryBiasOptions? biasOptions = null;
                if (biasScale != 1.0d || minStrongWins != 1 || minBias != 0.0001d)
                    biasOptions = new RouteMemoryBiasOptions(
                        MinStrongWinsToActivate: minStrongWins,
                        BiasScale: biasScale,
                        MinBiasToApply: minBias);
                return composition.GraphCommands.AgentQuery(
                    db,
                    q,
                    top,
                    explain,
                    recordRoute,
                    currentNode,
                    parentNode,
                    grandparentNode,
                    role,
                    mode,
                    failureBucket,
                    artifacts,
                    traversalBudget,
                    noActiveThreadContext,
                    biasOptions);
            }

            case "smartlist-create":
            {
                var db = p.GetRequired("db");
                var path = p.GetRequired("path");
                var durable = p.HasFlag("durable");
                var createdBy = p.GetOptional("created-by") ?? "memoryctl";
                return composition.GraphCommands.SmartListCreate(db, path, durable, createdBy);
            }

            case "smartlist-note":
            {
                var db = p.GetRequired("db");
                var title = p.GetRequired("title");
                var text = p.GetRequired("text");
                var inRaw = p.GetOptional("in");
                var bucketPaths = string.IsNullOrWhiteSpace(inRaw)
                    ? Array.Empty<string>()
                    : inRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                var durable = p.HasFlag("durable");
                var createdBy = p.GetOptional("created-by") ?? "memoryctl";
                return composition.GraphCommands.SmartListNote(db, title, text, bucketPaths, durable, createdBy);
            }

            case "smartlist-attach":
            {
                var db = p.GetRequired("db");
                var path = p.GetRequired("path");
                var member = p.GetRequired("member");
                var createdBy = p.GetOptional("created-by") ?? "memoryctl";
                return composition.GraphCommands.SmartListAttach(db, path, member, createdBy);
            }

            case "smartlist-inspect":
            {
                var db = p.GetRequired("db");
                var path = p.GetRequired("path");
                var depth = p.GetInt("depth", 1);
                return composition.GraphCommands.SmartListInspect(db, path, depth);
            }

            case "smartlist-remember":
            {
                var db = p.GetRequired("db");
                var path = p.GetOptional("path");
                var id = p.GetOptional("id");
                return composition.GraphCommands.SmartListRemember(db, path, id);
            }

            case "smartlist-rollup":
            {
                var db = p.GetRequired("db");
                var path = p.GetRequired("path");
                var summary = p.GetRequired("summary");
                var scope = p.GetRequired("scope");
                var stopHint = p.GetOptional("stop-hint");
                var durable = p.HasFlag("durable");
                var createdBy = p.GetOptional("created-by") ?? "memoryctl";
                var childRaw = p.GetOptional("child");
                var children = string.IsNullOrWhiteSpace(childRaw)
                    ? []
                    : childRaw
                        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        .Select(value =>
                        {
                            var sep = value.IndexOf("::", StringComparison.Ordinal);
                            if (sep <= 0 || sep + 2 >= value.Length)
                                throw new ArgumentException($"Invalid --child value '{value}'. Expected <smartlist/path>::<summary>.");
                            return new SmartListRollupChild(value[..sep], value[(sep + 2)..]);
                        })
                        .ToList();
                return composition.GraphCommands.SmartListRollup(db, path, summary, scope, stopHint, children, durable, createdBy);
            }

            case "smartlist-visibility":
            {
                var db = p.GetRequired("db");
                var path = p.GetRequired("path");
                var visibility = p.GetRequired("visibility");
                var recursive = p.HasFlag("recursive");
                var includeNotes = p.HasFlag("include-notes");
                var includeRollups = p.HasFlag("include-rollups");
                return composition.GraphCommands.SmartListVisibility(db, path, visibility, recursive, includeNotes, includeRollups);
            }

            case "smartlist-rollup-show":
            {
                var db = p.GetRequired("db");
                var path = p.GetRequired("path");
                return composition.GraphCommands.SmartListRollupShow(db, path);
            }

            case "bugreport-create":
            {
                var db = p.GetRequired("db");
                var sourceAgent = p.GetRequired("source-agent");
                var parentAgent = p.GetRequired("parent-agent");
                var errorOutput = p.GetRequired("error-output");
                var stackContext = p.GetRequired("stack-context");
                var severity = p.GetRequired("severity");
                var durable = p.HasFlag("durable");
                var createdBy = p.GetOptional("created-by") ?? "memoryctl";
                var attemptedFixRaw = p.GetOptional("attempted-fix");
                var attemptedFixes = string.IsNullOrWhiteSpace(attemptedFixRaw)
                    ? (IReadOnlyList<string>)[]
                    : attemptedFixRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
                var reproStepRaw = p.GetOptional("reproduction-step");
                var reproSteps = string.IsNullOrWhiteSpace(reproStepRaw)
                    ? (IReadOnlyList<string>)[]
                    : reproStepRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
                var recommendedFixPlan = p.GetOptional("recommended-fix-plan");
                return composition.GraphCommands.BugReportCreate(
                    db, sourceAgent, parentAgent, errorOutput, stackContext,
                    attemptedFixes, reproSteps, recommendedFixPlan, severity, durable, createdBy);
            }

            case "bugreport-update-status":
            {
                var db = p.GetRequired("db");
                var bugId = p.GetRequired("bug-id");
                var status = p.GetRequired("status");
                return composition.GraphCommands.BugReportUpdateStatus(db, bugId, status);
            }

            case "bugreport-show":
            {
                var db = p.GetRequired("db");
                var bugId = p.GetRequired("bug-id");
                return composition.GraphCommands.BugReportShow(db, bugId);
            }

            case "bugreport-list":
            {
                var db = p.GetRequired("db");
                var statusFilter = p.GetOptional("status");
                return composition.GraphCommands.BugReportList(db, statusFilter);
            }

            case "bugreport-search":
            {
                var db = p.GetRequired("db");
                var query = p.GetRequired("query");
                var statusFilter = p.GetOptional("status");
                return composition.GraphCommands.BugReportSearch(db, query, statusFilter);
            }

            case "bugfix-create":
            {
                var db = p.GetRequired("db");
                var title = p.GetRequired("title");
                var description = p.GetRequired("description");
                var fixRecipe = p.GetRequired("fix-recipe");
                var linkedBugreport = p.GetOptional("linked-bugreport");
                var durable = p.HasFlag("durable");
                var createdBy = p.GetOptional("created-by") ?? "memoryctl";
                return composition.GraphCommands.BugFixCreate(db, title, description, fixRecipe, linkedBugreport, durable, createdBy);
            }

            case "bugfix-show":
            {
                var db = p.GetRequired("db");
                var fixId = p.GetRequired("fix-id");
                return composition.GraphCommands.BugFixShow(db, fixId);
            }

            case "bugfix-list":
            {
                var db = p.GetRequired("db");
                return composition.GraphCommands.BugFixList(db);
            }

            case "bugfix-link":
            {
                var db = p.GetRequired("db");
                var bugId = p.GetRequired("bug-id");
                var fixId = p.GetRequired("fix-id");
                var createdBy = p.GetOptional("created-by") ?? "memoryctl";
                return composition.GraphCommands.BugFixLink(db, bugId, fixId, createdBy);
            }

            case "thread-status":
            {
                var db = p.GetRequired("db");
                return composition.GraphCommands.ThreadStatus(db);
            }

            case "thread-start":
            {
                var db = p.GetRequired("db");
                var id = p.GetOptional("id");
                var title = p.GetRequired("title");
                var currentStep = p.GetRequired("current-step");
                var nextCommand = p.GetRequired("next-command");
                var branchOffAnchor = p.GetOptional("branch-off-anchor");
                var artifactRef = p.GetOptional("artifact-ref");
                return composition.GraphCommands.ThreadStart(db, title, currentStep, nextCommand, id, branchOffAnchor, artifactRef);
            }

            case "thread-push-tangent":
            {
                var db = p.GetRequired("db");
                var id = p.GetOptional("id");
                var title = p.GetRequired("title");
                var currentStep = p.GetRequired("current-step");
                var nextCommand = p.GetRequired("next-command");
                var branchOffAnchor = p.GetOptional("branch-off-anchor");
                var artifactRef = p.GetOptional("artifact-ref");
                return composition.GraphCommands.ThreadPushTangent(db, title, currentStep, nextCommand, id, branchOffAnchor, artifactRef);
            }

            case "thread-checkpoint":
            {
                var db = p.GetRequired("db");
                var currentStep = p.GetRequired("current-step");
                var nextCommand = p.GetRequired("next-command");
                var branchOffAnchor = p.GetOptional("branch-off-anchor");
                var artifactRef = p.GetOptional("artifact-ref");
                return composition.GraphCommands.ThreadCheckpoint(db, currentStep, nextCommand, branchOffAnchor, artifactRef);
            }

            case "thread-pop":
            {
                var db = p.GetRequired("db");
                return composition.GraphCommands.ThreadPop(db);
            }

            case "thread-archive":
            {
                var db = p.GetRequired("db");
                var id = p.GetOptional("id");
                return composition.GraphCommands.ThreadArchive(db, id);
            }

            case "thread-list":
            {
                var db = p.GetRequired("db");
                return composition.GraphCommands.ThreadList(db);
            }

            case "agent-capability-upsert":
            {
                var db = p.GetRequired("db");
                var agent = p.GetRequired("agent");
                var capabilityKey = p.GetRequired("capability-key");
                var state = p.GetRequired("state");
                var problemKey = p.GetRequired("problem-key");
                var equivalenceGroupKey = p.GetRequired("equivalence-group-key");
                var summary = p.GetOptional("summary");
                var notes = p.GetOptional("notes");
                var createdBy = p.GetOptional("created-by") ?? "memoryctl";
                return composition.GraphCommands.AgentCapabilityUpsert(db, agent, capabilityKey, state, problemKey, equivalenceGroupKey, summary, notes, createdBy);
            }

            case "agent-capability-show":
            {
                var db = p.GetRequired("db");
                var id = p.GetOptional("id");
                var agent = p.GetOptional("agent");
                var capabilityKey = p.GetOptional("capability-key");
                return composition.GraphCommands.AgentCapabilityShow(db, id, agent, capabilityKey);
            }

            case "agent-capability-list":
            {
                var db = p.GetRequired("db");
                var agent = p.GetOptional("agent");
                var problem = p.GetOptional("problem");
                var group = p.GetOptional("group");
                return composition.GraphCommands.AgentCapabilityList(db, agent, problem, group);
            }

            case "relay-run":
            {
                var db = p.GetRequired("db");
                var taskThreadId = p.GetRequired("task-thread-id");
                var injectHandoffFailure = p.GetOptional("inject-handoff-failure");
                return composition.GraphCommands.RelayRun(db, taskThreadId, injectHandoffFailure);
            }

            case "route-replay":
            {
                var db = p.GetRequired("db");
                var input = p.GetRequired("input");
                var outPath = p.GetRequired("out");
                var top = p.GetInt("top", 8);
                return composition.GraphCommands.RouteReplay(db, input, outPath, top);
            }

            default:
                PrintHelp();
                return 1;
        }
    }
}

internal readonly record struct CommandRunResult(int ExitCode, string Stdout, string Stderr);

internal readonly record struct ShadowCompareResult(int ExitCode, string Message);

internal static class ShadowCompareReporter
{
    public static ShadowCompareResult BuildReport(string command, CommandRunResult legacy, CommandRunResult ams)
    {
        if (ams.ExitCode != 0)
        {
            var message =
                "[shadow-compare] AMS execution failed." + Environment.NewLine +
                $"  command: {command}" + Environment.NewLine +
                $"  legacy_exit: {legacy.ExitCode}" + Environment.NewLine +
                $"  ams_exit: {ams.ExitCode}" + Environment.NewLine +
                $"  ams_stderr: {TrimForSingleLine(ams.Stderr)}";
            return new ShadowCompareResult(3, message);
        }

        var diffs = new List<string>();
        AppendLineDiffs(diffs, "stdout", legacy.Stdout, ams.Stdout);
        AppendLineDiffs(diffs, "stderr", legacy.Stderr, ams.Stderr);

        if (diffs.Count == 0)
            return new ShadowCompareResult(0, $"[shadow-compare] MATCH command={command} backend=legacy vs backend=ams");

        var lines = new List<string>
        {
            $"[shadow-compare] DIFF command={command} backend=legacy vs backend=ams",
            $"  legacy_exit={legacy.ExitCode} ams_exit={ams.ExitCode}"
        };
        lines.AddRange(diffs);

        return new ShadowCompareResult(3, string.Join(Environment.NewLine, lines));
    }

    private static void AppendLineDiffs(List<string> diffs, string streamName, string left, string right)
    {
        var legacyLines = NormalizeLines(left);
        var amsLines = NormalizeLines(right);
        var max = Math.Max(legacyLines.Length, amsLines.Length);

        var shown = 0;
        for (var i = 0; i < max; i++)
        {
            var l = i < legacyLines.Length ? legacyLines[i] : "<missing>";
            var r = i < amsLines.Length ? amsLines[i] : "<missing>";
            if (string.Equals(l, r, StringComparison.Ordinal))
                continue;

            diffs.Add($"  [{streamName}] line {i + 1}: legacy='{l}' ams='{r}'");
            shown++;
            if (shown >= 8)
                break;
        }

        if (shown == 0 && !string.Equals(left, right, StringComparison.Ordinal))
            diffs.Add($"  [{streamName}] content differs but no line-level diff was produced.");
    }

    private static string[] NormalizeLines(string value)
        => value.Replace("\r\n", "\n", StringComparison.Ordinal)
                .Replace('\r', '\n')
                .Split('\n', StringSplitOptions.RemoveEmptyEntries);

    private static string TrimForSingleLine(string text)
    {
        var trimmed = text.Replace("\r", " ", StringComparison.Ordinal)
            .Replace("\n", " ", StringComparison.Ordinal)
            .Trim();

        return trimmed.Length <= 200 ? trimmed : trimmed[..200] + "...";
    }
}



