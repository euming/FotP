using FotP.Engine.Tiles;

namespace FotP.Engine.Market;

public static class TileDealer
{
    /// <summary>
    /// Deal tiles for a game given bar slot requests.
    /// For first-game setup, each BarSlotRequest has a FixedTileId.
    /// For random games, FixedTileId is null and tiles are drawn randomly.
    /// </summary>
    public static TileMarket Deal(
        IReadOnlyList<BarSlotRequest> slotRequests,
        int playerCount,
        Random rng)
    {
        if (playerCount < 2 || playerCount > 4)
            throw new ArgumentOutOfRangeException(nameof(playerCount),
                "Player count must be 2-4.");

        var pool = new TilePool();
        var slots = new List<MarketSlot>();

        foreach (var request in slotRequests)
        {
            TileDefinition tileDef = request.FixedTileId != null
                ? pool.DrawSpecific(request.FixedTileId)
                : pool.DrawRandom(request.Color, request.Level, rng);

            slots.Add(new MarketSlot(tileDef, playerCount));
        }

        return new TileMarket(slots);
    }

    /// <summary>
    /// Validates that a set of bar slot requests can be fulfilled by the tile catalog.
    /// Returns a list of validation errors (empty = valid).
    /// </summary>
    public static IReadOnlyList<string> Validate(IReadOnlyList<BarSlotRequest> slotRequests)
    {
        var errors = new List<string>();
        var pool = new TilePool();

        // Check for duplicate fixed tile IDs
        var fixedIds = slotRequests
            .Where(r => r.FixedTileId != null)
            .Select(r => r.FixedTileId!)
            .ToList();
        var duplicateFixed = fixedIds.GroupBy(id => id)
            .Where(g => g.Count() > 1).Select(g => g.Key);
        foreach (var dup in duplicateFixed)
            errors.Add($"Duplicate fixed tile ID: {dup}");

        // Check pool capacity per (color, level) for random slots
        var randomSlots = slotRequests
            .Where(r => r.FixedTileId == null)
            .GroupBy(r => (r.Color, r.Level));
        foreach (var group in randomSlots)
        {
            int needed = group.Count();
            int available = pool.Remaining(group.Key.Color, group.Key.Level);
            int fixedAtSameGroup = slotRequests
                .Count(r => r.FixedTileId != null
                    && r.Color == group.Key.Color
                    && r.Level == group.Key.Level);
            int effectiveAvailable = available - fixedAtSameGroup;
            if (needed > effectiveAvailable)
                errors.Add($"Need {needed} random {group.Key.Color} tiles at level {group.Key.Level} but only {effectiveAvailable} available after fixed draws.");
        }

        return errors;
    }
}
