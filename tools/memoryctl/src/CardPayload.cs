using System;

namespace MemoryCtl;

public sealed record CardPayload(
    Guid CardId,
    string? Title,
    string? Text,
    string? Source,
    DateTimeOffset? UpdatedAt
);
