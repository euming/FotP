using FotP.Engine.Market;
using FotP.Engine.Tiles;

namespace FotP.Engine.Tests.Market;

public class MarketSlotTests
{
    private static MarketSlot MakeSlot(int playerCount = 3)
        => new(TileCatalog.GetById("farmer"), playerCount);

    [Fact]
    public void Constructor_CreatesCorrectStackSize()
        => Assert.Equal(2, MakeSlot(3).Stack.Count);

    [Fact]
    public void ClaimTile_ReturnsTopTile()
    {
        var slot = MakeSlot(3);
        var tile = slot.ClaimTile();
        Assert.NotNull(tile);
        Assert.Equal("Farmer", tile!.Name);
    }

    [Fact]
    public void ClaimTile_ReducesStackCount()
    {
        var slot = MakeSlot(3);
        slot.ClaimTile();
        Assert.Equal(1, slot.Stack.Count);
    }

    [Fact]
    public void ClaimTile_ReturnsNull_WhenExhausted()
    {
        var slot = MakeSlot(2); // 1 tile
        slot.ClaimTile();
        Assert.Null(slot.ClaimTile());
    }

    [Fact]
    public void IsExhausted_TrueAfterAllClaimed()
    {
        var slot = MakeSlot(2);
        slot.ClaimTile();
        Assert.True(slot.IsExhausted);
    }
}
