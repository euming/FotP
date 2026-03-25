using FotP.Engine.Tiles;

namespace FotP.Engine.Market;

public class TileMarket
{
    private readonly List<MarketSlot> _slots;

    public TileMarket(IEnumerable<MarketSlot> slots)
    {
        _slots = slots.ToList();
    }

    public IReadOnlyList<MarketSlot> AllSlots => _slots;

    public IReadOnlyList<MarketSlot> GetSlotsByLevel(int level)
        => _slots.Where(s => s.Level == level).ToList();

    public IReadOnlyList<MarketSlot> GetSlotsByColor(TileColor color)
        => _slots.Where(s => s.Color == color).ToList();

    public IReadOnlyList<MarketSlot> GetAvailableSlots()
        => _slots.Where(s => !s.IsExhausted).ToList();

    public MarketSlot? FindSlotByTileId(string tileId)
        => _slots.FirstOrDefault(s => s.TileType.Id == tileId);
}
