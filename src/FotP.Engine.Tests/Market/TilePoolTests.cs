using FotP.Engine.Market;
using FotP.Engine.Tiles;

namespace FotP.Engine.Tests.Market;

public class TilePoolTests
{
    [Fact]
    public void DrawRandom_ReturnsTileOfCorrectColorAndLevel()
    {
        var pool = new TilePool();
        var tile = pool.DrawRandom(TileColor.Yellow, 3, new Random(42));
        Assert.Equal(TileColor.Yellow, tile.Color);
        Assert.Equal(3, tile.Level);
    }

    [Fact]
    public void DrawRandom_ReducesRemainingCount()
    {
        var pool = new TilePool();
        int before = pool.Remaining(TileColor.Yellow, 3);
        pool.DrawRandom(TileColor.Yellow, 3, new Random(1));
        Assert.Equal(before - 1, pool.Remaining(TileColor.Yellow, 3));
    }

    [Fact]
    public void DrawRandom_ThrowsWhenExhausted()
    {
        var pool = new TilePool();
        // Blue L3 has only 1 tile (Soothsayer)
        pool.DrawRandom(TileColor.Blue, 3, new Random(0));
        Assert.Throws<InvalidOperationException>(() =>
            pool.DrawRandom(TileColor.Blue, 3, new Random(0)));
    }

    [Fact]
    public void DrawSpecific_ReturnsTileAndRemovesFromPool()
    {
        var pool = new TilePool();
        int before = pool.TotalRemaining;
        var tile = pool.DrawSpecific("farmer");
        Assert.Equal("farmer", tile.Id);
        Assert.Equal(before - 1, pool.TotalRemaining);
    }

    [Fact]
    public void DrawSpecific_ThrowsForMissingId()
    {
        var pool = new TilePool();
        Assert.Throws<KeyNotFoundException>(() => pool.DrawSpecific("no-such-tile"));
    }

    [Fact]
    public void DrawRandom_WithSeed_IsDeterministic()
    {
        var pool1 = new TilePool();
        var pool2 = new TilePool();
        var tile1 = pool1.DrawRandom(TileColor.Yellow, 3, new Random(999));
        var tile2 = pool2.DrawRandom(TileColor.Yellow, 3, new Random(999));
        Assert.Equal(tile1.Id, tile2.Id);
    }

    [Fact]
    public void TotalRemaining_DecreasesAfterDraw()
    {
        var pool = new TilePool();
        int before = pool.TotalRemaining;
        pool.DrawRandom(TileColor.Red, 5, new Random(7));
        Assert.Equal(before - 1, pool.TotalRemaining);
    }

    [Fact]
    public void CustomPoolConstructor_WorksWithSubset()
    {
        var tiles = TileCatalog.ByColorAndLevel(TileColor.Yellow, 3);
        var pool = new TilePool(tiles);
        Assert.Equal(7, pool.TotalRemaining);
        Assert.Equal(0, pool.Remaining(TileColor.Blue, 3));
    }
}
