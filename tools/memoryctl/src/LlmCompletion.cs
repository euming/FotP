using System;

namespace MemoryCtl;

internal sealed record LlmCompletion(
    string Channel,
    string ChatId,
    string CompletionMessageId,
    DateTimeOffset Ts,
    string? ParentMessageId,
    string? Model,
    string Text);
