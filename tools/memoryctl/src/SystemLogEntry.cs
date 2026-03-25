using System;

namespace MemoryCtl;

internal sealed record SystemLogEntry(
    string SourceFile,
    string JobName,
    DateTimeOffset Ts,
    string Text,
    bool IsError,
    bool IsWarning);
