using FotP.Engine.Market;
using FotP.Engine.Tiles;

namespace FotP.Engine.Tests.Market;

public class TileCatalogTests
{
    [Fact]
    public void CatalogHas54TileTypes()
        => Assert.Equal(54, TileCatalog.All.Count);

    [Fact]
    public void YellowCount_Is27()
        => Assert.Equal(27, TileCatalog.ByColor(TileColor.Yellow).Count);

    [Fact]
    public void BlueCount_Is12()
        => Assert.Equal(12, TileCatalog.ByColor(TileColor.Blue).Count);

    [Fact]
    public void RedCount_Is15()
        => Assert.Equal(15, TileCatalog.ByColor(TileColor.Red).Count);

    [Theory]
    [InlineData(3, 11)]
    [InlineData(4, 11)]
    [InlineData(5, 11)]
    [InlineData(6, 11)]
    [InlineData(7, 10)]
    public void LevelCounts_AreCorrect(int level, int expected)
        => Assert.Equal(expected, TileCatalog.ByLevel(level).Count);

    [Fact]
    public void AllIdsAreUnique()
    {
        var ids = TileCatalog.All.Select(t => t.Id).ToList();
        Assert.Equal(ids.Count, ids.Distinct().Count());
    }

    [Fact]
    public void GetById_ReturnsCorrectTile()
    {
        var farmer = TileCatalog.GetById("farmer");
        Assert.Equal("Farmer", farmer.Name);
        Assert.Equal(3, farmer.Level);
        Assert.Equal(TileColor.Yellow, farmer.Color);

        var queen = TileCatalog.GetById("queen");
        Assert.Equal(7, queen.Level);
        Assert.Equal(TileColor.Yellow, queen.Color);
    }

    [Fact]
    public void GetById_ThrowsForUnknownId()
        => Assert.Throws<KeyNotFoundException>(() => TileCatalog.GetById("nonexistent-tile"));

    [Fact]
    public void ByColorAndLevel_YellowL3_Returns7()
        => Assert.Equal(7, TileCatalog.ByColorAndLevel(TileColor.Yellow, 3).Count);

    [Fact]
    public void ByColorAndLevel_BlueL3_Returns1()
        => Assert.Equal(1, TileCatalog.ByColorAndLevel(TileColor.Blue, 3).Count);
}
