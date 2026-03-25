using FotP.Engine.Criteria;
using FotP.Engine.Dice;

namespace FotP.Engine.Tests.Criteria;

public class CriterionTests
{
    private static List<Die> MakeDice(params int[] values)
    {
        return values.Select(v =>
        {
            var d = new Die(DieType.Standard);
            d.SetValue(v);
            return d;
        }).ToList();
    }

    [Fact]
    public void NOfAKind_2_MatchesPair()
    {
        var c = new NOfAKind(2);
        Assert.True(c.Evaluate(MakeDice(3, 3, 5)));
    }

    [Fact]
    public void NOfAKind_3_FailsOnlyPair()
    {
        var c = new NOfAKind(3);
        Assert.False(c.Evaluate(MakeDice(3, 3, 5)));
    }

    [Fact]
    public void NOfAKind_3_MatchesTriple()
    {
        var c = new NOfAKind(3);
        Assert.True(c.Evaluate(MakeDice(4, 4, 4, 2)));
    }

    [Fact]
    public void Straight_3_MatchesConsecutive()
    {
        var c = new Straight(3);
        Assert.True(c.Evaluate(MakeDice(2, 3, 4)));
    }

    [Fact]
    public void Straight_3_FailsNonConsecutive()
    {
        var c = new Straight(3);
        Assert.False(c.Evaluate(MakeDice(1, 3, 5)));
    }

    [Fact]
    public void Straight_3_MatchesWithExtraValues()
    {
        var c = new Straight(3);
        Assert.True(c.Evaluate(MakeDice(1, 2, 3, 4, 6)));
    }

    [Fact]
    public void SumGreaterEqual_MatchesWhenMet()
    {
        var c = new SumGreaterEqual(10);
        Assert.True(c.Evaluate(MakeDice(4, 3, 4)));
    }

    [Fact]
    public void SumGreaterEqual_FailsWhenUnder()
    {
        var c = new SumGreaterEqual(10);
        Assert.False(c.Evaluate(MakeDice(2, 2, 2)));
    }

    [Fact]
    public void AllDifferent_MatchesUniqueValues()
    {
        var c = new AllDifferent();
        Assert.True(c.Evaluate(MakeDice(1, 2, 3, 4, 5, 6)));
    }

    [Fact]
    public void AllDifferent_FailsWithDuplicates()
    {
        var c = new AllDifferent();
        Assert.False(c.Evaluate(MakeDice(1, 2, 2)));
    }

    [Fact]
    public void TwoPairs_MatchesTwoPairs()
    {
        var c = new TwoPairs();
        Assert.True(c.Evaluate(MakeDice(2, 2, 4, 4)));
    }

    [Fact]
    public void TwoPairs_FailsOnePair()
    {
        var c = new TwoPairs();
        Assert.False(c.Evaluate(MakeDice(3, 3, 5)));
    }

    [Fact]
    public void FullHouse_MatchesCorrectly()
    {
        var c = new FullHouse();
        Assert.True(c.Evaluate(MakeDice(3, 3, 3, 5, 5)));
    }

    [Fact]
    public void FullHouse_FailsOnSingleKind()
    {
        var c = new FullHouse();
        // Only one group — no second group at all
        Assert.False(c.Evaluate(MakeDice(4, 4, 4)));
    }

    [Fact]
    public void AllEven_MatchesAllEven()
    {
        var c = new AllEven();
        Assert.True(c.Evaluate(MakeDice(2, 4, 6)));
    }

    [Fact]
    public void AllOdd_MatchesAllOdd()
    {
        var c = new AllOdd();
        Assert.True(c.Evaluate(MakeDice(1, 3, 5)));
    }

    [Fact]
    public void CompoundCriteria_BothMustPass()
    {
        var c = new CompoundCriteria(new NOfAKind(2), new SumGreaterEqual(8));
        Assert.True(c.Evaluate(MakeDice(4, 4, 2)));   // pair=4+4=8 sum=10
        Assert.False(c.Evaluate(MakeDice(1, 1, 2)));  // pair but sum=4
    }

    [Fact]
    public void EmptyDice_AllCriteria_ReturnFalse()
    {
        var dice = new List<Die>();
        Assert.False(new NOfAKind(1).Evaluate(dice));
        Assert.False(new AllDifferent().Evaluate(dice));
        Assert.False(new AllEven().Evaluate(dice));
    }
}
