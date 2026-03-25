using FotP.Engine.Dice;

namespace FotP.Engine.Tests.Dice;

public class DieTests
{
    [Fact]
    public void StandardDie_HasSixFaces_OneThroughSix()
    {
        var die = new Die(DieType.Standard);
        var faces = die.GetFaces();
        Assert.Equal(6, faces.Length);
        for (int i = 1; i <= 6; i++)
            Assert.Contains(i, faces);
    }

    [Fact]
    public void Die_Roll_ChangesValue()
    {
        var die = new Die(DieType.Standard);
        var rng = new Random(42);
        var values = new HashSet<int>();
        for (int i = 0; i < 50; i++)
        {
            die.Roll(rng);
            values.Add(die.Value);
        }
        // With 50 rolls, expect multiple distinct values
        Assert.True(values.Count > 1);
    }

    [Fact]
    public void Die_SetValue_UpdatesPipValue()
    {
        var die = new Die(DieType.Standard);
        die.SetValue(4);
        Assert.Equal(4, die.PipValue);
        Assert.True(die.HasPipValue);
    }

    [Fact]
    public void Die_SetValue_InvalidFace_Throws()
    {
        var die = new Die(DieType.Standard);
        Assert.Throws<ArgumentException>(() => die.SetValue(99));
    }

    [Fact]
    public void ArtisanDie_HasStarFace()
    {
        var die = new Die(DieType.Artisan);
        var faces = die.GetFaces();
        Assert.Contains(DieFaces.StarFace, faces);
    }

    [Fact]
    public void NobleDie_HasHighPipFaces()
    {
        var die = new Die(DieType.Noble);
        var faces = die.GetFaces();
        // Noble die faces: 5, 6, 3, 4, 5, 6 — no pips below 3
        Assert.All(faces, f => Assert.True(f >= 3));
    }

    [Theory]
    [InlineData(DieType.Standard)]
    [InlineData(DieType.Noble)]
    [InlineData(DieType.Artisan)]
    [InlineData(DieType.Serf)]
    public void AllDieTypes_HaveSixFaces(DieType type)
    {
        var die = new Die(type);
        Assert.Equal(6, die.GetFaces().Length);
    }

    [Fact]
    public void Die_TempPipModifier_AffectsPipValue()
    {
        var die = new Die(DieType.Standard);
        die.SetValue(3);
        die.TempPipModifier = 2;
        Assert.Equal(5, die.PipValue);
    }

    [Fact]
    public void Die_PipValue_ClampedToMaxValue()
    {
        var die = new Die(DieType.Standard);
        die.SetValue(6);
        die.TempPipModifier = 100;
        Assert.Equal(die.MaxValue, die.PipValue);
    }

    [Fact]
    public void ImmediateDie_MustLockImmediately()
    {
        var die = new Die(DieType.Immediate);
        Assert.True(die.MustLockImmediately);
    }

    [Fact]
    public void StandardDie_DoesNotMustLockImmediately()
    {
        var die = new Die(DieType.Standard);
        Assert.False(die.MustLockImmediately);
    }
}
