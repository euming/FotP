using System;

namespace MemoryCtl;

public sealed record ChatEvent(
    string Channel,
    string ChatId,
    string MessageId,
    DateTimeOffset Ts,
    string? Author,
    string Direction,
    string Text,
    string? Slug = null,   // optional human-readable session name (e.g. "temporal-soaring-leaf")
    int TokensIn = 0,
    int TokensOut = 0,
    int TokensCacheRead = 0,
    int TokensCacheCreate = 0
);

public sealed record ToolCallEvent(
    string Channel,
    string ChatId,
    string ToolUseId,
    DateTimeOffset Ts,
    string ToolName,
    string InputJson,       // raw JSON string of the input object
    string? ResultPreview,
    bool IsError
);
