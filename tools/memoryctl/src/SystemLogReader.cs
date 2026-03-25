using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace MemoryCtl;

internal static class SystemLogReader
{
    private static readonly Regex TsLine = new(@"^\[(?<ts>\d{4}-\d{2}-\d{2}T[^\]]+)\]\s*$", RegexOptions.Compiled);

    public static IEnumerable<(int endLineNo, SystemLogEntry entry)> ReadEntries(string path, int startAfterLineNo, string jobName)
    {
        var fileName = Path.GetFileName(path);

        int lineNo = 0;
        DateTimeOffset? curTs = null;
        var sb = new StringBuilder();
        int lastEndLine = 0;

        foreach (var line in File.ReadLines(path))
        {
            lineNo++;
            if (lineNo <= startAfterLineNo) continue;

            var m = TsLine.Match(line.TrimEnd());
            if (m.Success)
            {
                // flush previous entry
                if (curTs != null && sb.Length > 0)
                {
                    var text = sb.ToString().Trim();
                    var (isErr, isWarn) = Classify(text);
                    yield return (lineNo - 1, new SystemLogEntry(fileName, jobName, curTs.Value, text, isErr, isWarn));
                    sb.Clear();
                }

                var tsStr = m.Groups["ts"].Value;
                if (!DateTimeOffset.TryParse(tsStr, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var ts))
                {
                    // If timestamp is malformed, treat as a continuation line.
                    sb.AppendLine(line);
                    curTs = curTs ?? DateTimeOffset.Now;
                    continue;
                }

                curTs = ts;
                lastEndLine = lineNo;
                continue;
            }

            if (curTs == null)
            {
                // If the file doesn't start with a timestamp marker, synthesize one.
                curTs = DateTimeOffset.Now;
            }

            sb.AppendLine(line);
        }

        // final flush
        if (curTs != null && sb.Length > 0)
        {
            var text = sb.ToString().Trim();
            var (isErr, isWarn) = Classify(text);
            yield return (lineNo, new SystemLogEntry(Path.GetFileName(path), jobName, curTs.Value, text, isErr, isWarn));
        }
    }

    private static (bool isError, bool isWarning) Classify(string text)
    {
        var t = text;
        var isError = t.IndexOf("ERROR", StringComparison.OrdinalIgnoreCase) >= 0 ||
                      t.IndexOf("failed", StringComparison.OrdinalIgnoreCase) >= 0 ||
                      t.IndexOf("exception", StringComparison.OrdinalIgnoreCase) >= 0;
        var isWarning = !isError && (t.IndexOf("WARN", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                     t.IndexOf("warning", StringComparison.OrdinalIgnoreCase) >= 0);
        return (isError, isWarning);
    }
}
