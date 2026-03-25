using System.Text.RegularExpressions;

namespace MemoryCtl.Inspection;

internal static class InspectionTitlePolicy
{
    private static readonly HashSet<string> Stopwords = new(StringComparer.OrdinalIgnoreCase)
    {
        "a", "an", "the", "and", "or", "but", "if", "then", "else", "of", "to", "in", "on", "at", "for", "with", "by",
        "is", "are", "was", "were", "be", "been", "being", "do", "does", "did", "have", "has", "had", "it", "this", "that"
    };

    public static (string Title, string Quality) ResolveSessionTitle(
        string? explicitEnriched,
        string? threadTitle,
        string? chatTitle,
        string? fallbackText,
        DateTimeOffset startedAt,
        int sessionCount)
    {
        if (TryNormalize(explicitEnriched, out var explicitTitle))
            return (explicitTitle, "high");

        if (TryNormalize(threadTitle, out var groupTitle))
            return (groupTitle, "high");

        if (TryNormalize(chatTitle, out var normalizedChat))
            return (normalizedChat, "high");

        if (TryBuildKeywordFallback(fallbackText, out var fallback))
            return (fallback, "fallback");

        var safe = startedAt == DateTimeOffset.MinValue
            ? $"Session group ({sessionCount})"
            : $"Session {startedAt:yyyy-MM-dd HH:mm}";
        return (safe, "invalid");
    }

    public static (string Label, string Quality) ResolveDreamLabel(string? summary, string objectId, string kind)
    {
        if (TryNormalize(summary, out var normalized))
            return (normalized, "high");

        var suffix = objectId.Contains(':', StringComparison.Ordinal)
            ? objectId[(objectId.IndexOf(':', StringComparison.Ordinal) + 1)..]
            : objectId;
        var shortSuffix = suffix.Length > 8 ? suffix[..8] : suffix;
        return ($"{kind} {shortSuffix}", "invalid");
    }

    public static bool IsStructuralMembersId(string id)
        => id.Contains("-members:", StringComparison.OrdinalIgnoreCase);

    private static bool TryNormalize(string? raw, out string normalized)
    {
        normalized = string.Empty;
        if (string.IsNullOrWhiteSpace(raw))
            return false;

        var candidate = raw.Trim();
        if (candidate.Contains(" | ", StringComparison.Ordinal))
            candidate = candidate[(candidate.IndexOf(" | ", StringComparison.Ordinal) + 3)..].Trim();

        candidate = Regex.Replace(candidate, "\\s+", " ");
        if (candidate.Length < 3)
            return false;

        if (LooksLikeOpaqueId(candidate))
            return false;

        if (IsLowSignal(candidate))
            return false;

        normalized = candidate;
        return true;
    }

    private static bool TryBuildKeywordFallback(string? text, out string fallback)
    {
        fallback = string.Empty;
        if (string.IsNullOrWhiteSpace(text))
            return false;

        var tokens = Regex.Matches(text.ToLowerInvariant(), "[a-z0-9]{3,}")
            .Select(m => m.Value)
            .Where(t => !Stopwords.Contains(t))
            .Distinct(StringComparer.Ordinal)
            .Take(4)
            .ToList();

        if (tokens.Count == 0)
            return false;

        fallback = string.Join(' ', tokens);
        return true;
    }

    private static bool IsLowSignal(string value)
    {
        var tokens = Regex.Matches(value.ToLowerInvariant(), "[a-z0-9]+").Select(m => m.Value).ToList();
        if (tokens.Count == 0)
            return true;

        return tokens.All(t => Stopwords.Contains(t));
    }

    private static bool LooksLikeOpaqueId(string value)
    {
        var candidate = value.Trim();
        candidate = Regex.Replace(candidate, "^session\\s*:\\s*", string.Empty, RegexOptions.IgnoreCase);

        if (Regex.IsMatch(candidate, "^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}(?:\\b|\\s|\\()", RegexOptions.IgnoreCase))
            return true;

        if (Regex.IsMatch(candidate, "^[0-9a-f]{8,}(?:\\b|\\s|\\()", RegexOptions.IgnoreCase))
            return true;

        var tokens = Regex.Matches(candidate, "[a-z0-9]+", RegexOptions.IgnoreCase)
            .Select(m => m.Value)
            .ToList();

        if (tokens.Count >= 4 && tokens.All(t => Regex.IsMatch(t, "^[0-9a-f]+$", RegexOptions.IgnoreCase)))
            return true;

        return false;
    }
}
