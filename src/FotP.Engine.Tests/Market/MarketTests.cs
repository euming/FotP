using FotP.Engine.Market;
using FotP.Engine.Tiles;

namespace FotP.Engine.Tests.Market;

public class MarketTests
{
    private static TileMarket MakeMarket()
    {
        var slots = new[]
        {
            new MarketSlot(TileCatalog.GetById("farmer"),   3), // L3 Yellow
            new MarketSlot(TileCatalog.GetById("soothsayer"), 3), // L3 Blue
            new MarketSlot(TileCatalog.GetById("ankh"),     3), // L3 Red
            new MarketSlot(TileCatalog.GetById("artisan"),  3), // L4 Yellow
        };
        return new TileMarket(slots);
    }

    [Fact]
    public void GetSlotsByLevel_FiltersCorrectly()
    {
        var market = MakeMarket();
        Assert.Equal(3, market.GetSlotsByLevel(3).Count);
        Assert.Equal(1, market.GetSlotsByLevel(4).Count);
    }

    [Fact]
    public void GetSlotsByColor_FiltersCorrectly()
    {
        var market = MakeMarket();
        Assert.Equal(2, market.GetSlotsByColor(TileColor.Yellow).Count);
        Assert.Equal(1, market.GetSlotsByColor(TileColor.Blue).Count);
    }

    [Fact]
    public void GetAvailableSlots_ExcludesExhausted()
    {
        var market = MakeMarket();
        // Exhaust farmer slot (2 tiles for 3 players)
        var farmerSlot = market.FindSlotByTileId("farmer")!;
        farmerSlot.ClaimTile();
        farmerSlot.ClaimTile();
        Assert.True(farmerSlot.IsExhausted);
        Assert.Equal(3, market.GetAvailableSlots().Count);
    }

    [Fact]
    public void FindSlotByTileId_ReturnsCorrectSlot()
    {
        var market = MakeMarket();
        var slot = market.FindSlotByTileId("ankh");
        Assert.NotNull(slot);
        Assert.Equal("ankh", slot!.TileType.Id);
    }

    [Fact]
    public void FindSlotByTileId_ReturnsNull_ForMissingTile()
    {
        var market = MakeMarket();
        Assert.Null(market.FindSlotByTileId("queen"));
    }
}
