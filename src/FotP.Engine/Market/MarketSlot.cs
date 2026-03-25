using FotP.Engine.Core;
using FotP.Engine.Tiles;

namespace FotP.Engine.Market;

public class MarketSlot
{
    public int Level { get; }
    public TileColor Color { get; }
    public TileDefinition TileType { get; }
    public SmartList<Tile> Stack { get; }
    public bool IsExhausted => Stack.Count == 0;

    public MarketSlot(TileDefinition tileType, int playerCount)
    {
        Level = tileType.Level;
        Color = tileType.Color;
        TileType = tileType;
        Stack = new SmartList<Tile>();
        for (int i = 0; i < playerCount - 1; i++)
            Stack.Add(new Tile(tileType.Name, tileType.Level, tileType.Color));
    }

    /// <summary>Claims (pops) the top tile from the stack. Returns null if exhausted.</summary>
    public Tile? ClaimTile()
    {
        if (Stack.Count == 0) return null;
        var tile = Stack[Stack.Count - 1];
        Stack.Remove(tile);
        return tile;
    }
}
