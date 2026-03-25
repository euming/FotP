using FotP.Engine.Tiles;

namespace FotP.Engine.Market;

public class TilePool
{
    private readonly Dictionary<(TileColor Color, int Level), List<TileDefinition>> _pool;

    /// <summary>Creates a pool from the full TileCatalog.</summary>
    public TilePool()
    {
        _pool = TileCatalog.All
            .GroupBy(t => (t.Color, t.Level))
            .ToDictionary(g => g.Key, g => g.ToList());
    }

    /// <summary>Creates a pool from a custom set of tile definitions (for testing).</summary>
    public TilePool(IEnumerable<TileDefinition> tiles)
    {
        _pool = tiles
            .GroupBy(t => (t.Color, t.Level))
            .ToDictionary(g => g.Key, g => g.ToList());
    }

    /// <summary>
    /// Draws a random tile of the specified color and level, removing it from the pool.
    /// Throws InvalidOperationException if no tiles of that color+level remain.
    /// </summary>
    public TileDefinition DrawRandom(TileColor color, int level, Random rng)
    {
        var key = (color, level);
        if (!_pool.TryGetValue(key, out var available) || available.Count == 0)
            throw new InvalidOperationException(
                $"No {color} tiles at level {level} remaining in pool.");
        int index = rng.Next(available.Count);
        var tile = available[index];
        available.RemoveAt(index);
        return tile;
    }

    /// <summary>
    /// Draws a specific tile by ID, removing it from the pool.
    /// Throws KeyNotFoundException if the tile is not in the pool.
    /// </summary>
    public TileDefinition DrawSpecific(string tileId)
    {
        foreach (var kvp in _pool)
        {
            var tile = kvp.Value.FirstOrDefault(t => t.Id == tileId);
            if (tile != null)
            {
                kvp.Value.Remove(tile);
                return tile;
            }
        }
        throw new KeyNotFoundException($"Tile '{tileId}' not found in pool.");
    }

    /// <summary>How many tiles of this color+level remain.</summary>
    public int Remaining(TileColor color, int level)
    {
        var key = (color, level);
        return _pool.TryGetValue(key, out var list) ? list.Count : 0;
    }

    /// <summary>Total tiles remaining across all groups.</summary>
    public int TotalRemaining => _pool.Values.Sum(v => v.Count);
}
