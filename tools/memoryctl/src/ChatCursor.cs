using System;

namespace MemoryCtl;

public sealed record ChatCursor(
    int LastLineNumber,
    DateTimeOffset? LastTs,
    string? LastMessageId
);
