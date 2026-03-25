using FotP.Engine.Market;
using FotP.Engine.Tiles;

namespace FotP.Engine.Tests.Market;

public class TileDealerTests
{
    [Fact]
    public void Deal_FixedSetup_PlacesCorrectTiles()
    {
        var requests = new[]
        {
            new BarSlotRequest(3, TileColor.Yellow, "farmer"),
            new BarSlotRequest(3, TileColor.Blue,   "soothsayer"),
            new BarSlotRequest(3, TileColor.Red,    "ankh"),
        };
        var market = TileDealer.Deal(requests, 3, new Random(0));
        Assert.NotNull(market.FindSlotByTileId("farmer"));
        Assert.NotNull(market.FindSlotByTileId("soothsayer"));
        Assert.NotNull(market.FindSlotByTileId("ankh"));
    }

    [Fact]
    public void Deal_RandomSetup_FillsAllSlots()
    {
        var requests = Enumerable.Range(0, 5)
            .Select(_ => new BarSlotRequest(4, TileColor.Yellow))
            .ToList();
        var market = TileDealer.Deal(requests, 2, new Random(1));
        Assert.Equal(5, market.AllSlots.Count);
        Assert.All(market.AllSlots, s => Assert.NotNull(s.TileType));
    }

    [Fact]
    public void Deal_RandomSetup_NoDuplicateTileTypes()
    {
        // L4 Yellow has 6 types; draw 5
        var requests = Enumerable.Range(0, 5)
            .Select(_ => new BarSlotRequest(4, TileColor.Yellow))
            .ToList();
        var market = TileDealer.Deal(requests, 2, new Random(42));
        var ids = market.AllSlots.Select(s => s.TileType.Id).ToList();
        Assert.Equal(ids.Count, ids.Distinct().Count());
    }

    [Fact]
    public void Deal_RandomSetup_IsDeterministic()
    {
        var requests = new[]
        {
            new BarSlotRequest(3, TileColor.Yellow),
            new BarSlotRequest(3, TileColor.Yellow),
        };
        var m1 = TileDealer.Deal(requests, 2, new Random(123));
        var m2 = TileDealer.Deal(requests, 2, new Random(123));
        var ids1 = m1.AllSlots.Select(s => s.TileType.Id).ToArray();
        var ids2 = m2.AllSlots.Select(s => s.TileType.Id).ToArray();
        Assert.Equal(ids1, ids2);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    public void Deal_ThrowsForInvalidPlayerCount(int count)
    {
        var requests = new[] { new BarSlotRequest(3, TileColor.Yellow, "farmer") };
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            TileDealer.Deal(requests, count, new Random(0)));
    }

    [Fact]
    public void Deal_StackSize_EqualsPlayersMinusOne()
    {
        var requests = new[] { new BarSlotRequest(3, TileColor.Yellow, "farmer") };
        var market = TileDealer.Deal(requests, 4, new Random(0));
        Assert.Equal(3, market.AllSlots[0].Stack.Count);
    }

    [Fact]
    public void Validate_ReturnsEmpty_ForValidSetup()
    {
        var requests = new[]
        {
            new BarSlotRequest(3, TileColor.Yellow, "farmer"),
            new BarSlotRequest(3, TileColor.Yellow),
        };
        var errors = TileDealer.Validate(requests);
        Assert.Empty(errors);
    }

    [Fact]
    public void Validate_DetectsDuplicateFixedTiles()
    {
        var requests = new[]
        {
            new BarSlotRequest(3, TileColor.Yellow, "farmer"),
            new BarSlotRequest(3, TileColor.Yellow, "farmer"),
        };
        var errors = TileDealer.Validate(requests);
        Assert.Contains(errors, e => e.Contains("Duplicate fixed tile ID"));
    }

    [Fact]
    public void Validate_DetectsPoolExhaustion()
    {
        // L3 Yellow has 7 types; requesting 8 random should fail
        var requests = Enumerable.Range(0, 8)
            .Select(_ => new BarSlotRequest(3, TileColor.Yellow))
            .ToList();
        var errors = TileDealer.Validate(requests);
        Assert.Contains(errors, e => e.Contains("level 3"));
    }
}
