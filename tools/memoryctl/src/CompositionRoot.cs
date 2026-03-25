namespace MemoryCtl;

internal enum MemoryCtlBackend
{
    Legacy,
    Ams
}

internal sealed class MemoryCtlComposition
{
    public required GraphCommandModule GraphCommands { get; init; }
    public required IngestSyncCommandModule IngestSyncCommands { get; init; }
}

internal static class MemoryCtlCompositionRoot
{
    public static MemoryCtlComposition Build(string? backendRaw)
    {
        var backend = ParseBackend(backendRaw);

        ICommandRuntimeFactory runtimeFactory = backend switch
        {
            MemoryCtlBackend.Legacy => new LegacyCommandRuntimeFactory(),
            MemoryCtlBackend.Ams => new AmsCommandRuntimeFactory(),
            _ => throw new ArgumentOutOfRangeException(nameof(backend), backend, "Unsupported backend.")
        };

        return new MemoryCtlComposition
        {
            GraphCommands = new GraphCommandModule(runtimeFactory),
            IngestSyncCommands = new IngestSyncCommandModule(runtimeFactory)
        };
    }

    private static MemoryCtlBackend ParseBackend(string? backendRaw)
    {
        if (string.IsNullOrWhiteSpace(backendRaw))
            return MemoryCtlBackend.Ams;

        var normalized = backendRaw.Trim();

        if (string.Equals(normalized, "legacy", StringComparison.OrdinalIgnoreCase))
            return MemoryCtlBackend.Legacy;

        if (string.Equals(normalized, "ams", StringComparison.OrdinalIgnoreCase))
            return MemoryCtlBackend.Ams;

        throw new ArgumentException($"Unsupported --backend '{backendRaw}'. Supported values: legacy|ams.");
    }
}
