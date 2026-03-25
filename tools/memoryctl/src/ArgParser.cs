using System;
using System.Collections.Generic;

namespace MemoryCtl;

internal sealed class ArgParser
{
    public string? Command { get; private set; }
    public Dictionary<string, string?> Options { get; } = new(StringComparer.OrdinalIgnoreCase);

    public static ArgParser Parse(string[] args)
    {
        var p = new ArgParser();
        if (args.Length == 0) return p;

        for (int i = 0; i < args.Length; i++)
        {
            var a = args[i];
            if (!a.StartsWith("--"))
            {
                if (p.Command is null)
                {
                    p.Command = a;
                    continue;
                }

                throw new ArgumentException($"Unexpected argument '{a}'. Options must start with --.");
            }

            var key = a.Substring(2);
            string? val = null;
            if (i + 1 < args.Length && !args[i + 1].StartsWith("--"))
            {
                val = args[i + 1];
                i++;
            }
            p.Options[key] = val;
        }

        return p;
    }

    public string GetRequired(string key)
    {
        if (!Options.TryGetValue(key, out var v) || string.IsNullOrWhiteSpace(v))
            throw new ArgumentException($"Missing required option --{key}.");
        return v!;
    }

    public int GetInt(string key, int @default)
    {
        if (!Options.TryGetValue(key, out var v) || string.IsNullOrWhiteSpace(v)) return @default;
        if (!int.TryParse(v, out var n)) throw new ArgumentException($"Invalid int for --{key}: '{v}'");
        return n;
    }

    public double GetDouble(string key, double @default)
    {
        if (!Options.TryGetValue(key, out var v) || string.IsNullOrWhiteSpace(v)) return @default;
        if (!double.TryParse(v, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var n))
            throw new ArgumentException($"Invalid double for --{key}: '{v}'");
        return n;
    }

    public bool HasFlag(string key) => Options.ContainsKey(key) && Options[key] == null;

    public bool HasOption(string key) => Options.ContainsKey(key);

    public string? GetOptional(string key)
    {
        Options.TryGetValue(key, out var v);
        return v;
    }
}
