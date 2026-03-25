using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Criteria;
using FotP.Engine.Dice;
using FotP.Engine.Market;
using FotP.Engine.Players;
using FotP.Engine.State;
using FotP.Engine.Tests.Helpers;
using Xunit;

namespace FotP.Engine.Tests.Market;

/// <summary>
/// Stress tests for B-side bar configuration correctness and game simulation stability.
/// </summary>
public class LevelBarConfigBSideTests
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

    // ----------------------------------------------------------------
    // LevelBarConfig.Get() roundtrip
    // ----------------------------------------------------------------

    [Theory]
    [InlineData(3)] [InlineData(4)] [InlineData(5)] [InlineData(6)] [InlineData(7)]
    public void Get_BSide_ReturnsCorrectLevelAndSide(int level)
    {
        var bar = LevelBarConfig.Get(level, BarSide.B);
        Assert.Equal(level, bar.Level);
        Assert.Equal(BarSide.B, bar.Side);
    }

    [Theory]
    [InlineData(3)] [InlineData(4)] [InlineData(5)] [InlineData(6)] [InlineData(7)]
    public void Get_BSide_HasFiveSlots(int level)
    {
        var bar = LevelBarConfig.Get(level, BarSide.B);
        Assert.Equal(5, bar.SlotCriteria.Count);
    }

    [Theory]
    [InlineData(3)] [InlineData(4)] [InlineData(5)] [InlineData(6)] [InlineData(7)]
    public void Get_BSide_SlotCriteria_AreNonNull(int level)
    {
        var bar = LevelBarConfig.Get(level, BarSide.B);
        Assert.All(bar.SlotCriteria, c => Assert.NotNull(c));
    }

    // ----------------------------------------------------------------
    // Level 3 B-side slot correctness
    // ----------------------------------------------------------------

    [Fact]
    public void Level3B_Slot0_ThreeOfAKind_Passes()
    {
        var c = LevelBarConfig.Level3B.SlotCriteria[0]; // NOfAKind(3)
        Assert.True(c.Evaluate(MakeDice(4, 4, 4)));
    }

    [Fact]
    public void Level3B_Slot0_ThreeOfAKind_Fails()
    {
        var c = LevelBarConfig.Level3B.SlotCriteria[0];
        Assert.False(c.Evaluate(MakeDice(1, 2, 3)));
    }

    [Fact]
    public void Level3B_Slot1_AllDiceInRange1to4_Passes()
    {
        var c = LevelBarConfig.Level3B.SlotCriteria[1]; // AllDiceInRange(1,4)
        Assert.True(c.Evaluate(MakeDice(1, 2, 4)));
    }

    [Fact]
    public void Level3B_Slot1_AllDiceInRange1to4_Fails_WhenDie5Present()
    {
        var c = LevelBarConfig.Level3B.SlotCriteria[1];
        Assert.False(c.Evaluate(MakeDice(1, 2, 5)));
    }

    [Fact]
    public void Level3B_Slot4_SumGe11_Passes()
    {
        var c = LevelBarConfig.Level3B.SlotCriteria[4]; // SumGreaterEqual(11)
        Assert.True(c.Evaluate(MakeDice(4, 4, 4)));  // sum=12
    }

    [Fact]
    public void Level3B_Slot4_SumGe11_Fails()
    {
        var c = LevelBarConfig.Level3B.SlotCriteria[4];
        Assert.False(c.Evaluate(MakeDice(1, 2, 3)));  // sum=6
    }

    // ----------------------------------------------------------------
    // Level 4 B-side slot correctness
    // ----------------------------------------------------------------

    [Fact]
    public void Level4B_Slot2_TwoPairs_Passes()
    {
        var c = LevelBarConfig.Level4B.SlotCriteria[2]; // TwoPairs
        Assert.True(c.Evaluate(MakeDice(2, 2, 5, 5)));
    }

    [Fact]
    public void Level4B_Slot2_TwoPairs_Fails_OnSinglePair()
    {
        var c = LevelBarConfig.Level4B.SlotCriteria[2];
        Assert.False(c.Evaluate(MakeDice(2, 2, 3, 4)));
    }

    [Fact]
    public void Level4B_Slot3_PairOf6AndPairOf1_Passes()
    {
        var c = LevelBarConfig.Level4B.SlotCriteria[3]; // CompoundCriteria(PairOfValue(6), PairOfValue(1))
        Assert.True(c.Evaluate(MakeDice(6, 6, 1, 1)));
    }

    [Fact]
    public void Level4B_Slot3_PairOf6AndPairOf1_Fails_OnlyPairOf6()
    {
        var c = LevelBarConfig.Level4B.SlotCriteria[3];
        Assert.False(c.Evaluate(MakeDice(6, 6, 2, 3)));
    }

    [Fact]
    public void Level4B_Slot4_SumGe20_Passes()
    {
        var c = LevelBarConfig.Level4B.SlotCriteria[4]; // SumGreaterEqual(20)
        Assert.True(c.Evaluate(MakeDice(5, 5, 5, 6))); // sum=21
    }

    [Fact]
    public void Level4B_Slot4_SumGe20_Fails()
    {
        var c = LevelBarConfig.Level4B.SlotCriteria[4];
        Assert.False(c.Evaluate(MakeDice(1, 2, 3, 4))); // sum=10
    }

    // ----------------------------------------------------------------
    // Level 5 B-side slot correctness
    // ----------------------------------------------------------------

    [Fact]
    public void Level5B_Slot2_ThreeSixes_Passes()
    {
        var c = LevelBarConfig.Level5B.SlotCriteria[2]; // NOfValue(3,6)
        Assert.True(c.Evaluate(MakeDice(6, 6, 6, 1, 2)));
    }

    [Fact]
    public void Level5B_Slot2_ThreeSixes_Fails_TwoSixes()
    {
        var c = LevelBarConfig.Level5B.SlotCriteria[2];
        Assert.False(c.Evaluate(MakeDice(6, 6, 1, 2, 3)));
    }

    [Fact]
    public void Level5B_Slot3_TwoThreeOfAKind_Passes()
    {
        var c = LevelBarConfig.Level5B.SlotCriteria[3]; // TwoNOfAKind(3)
        Assert.True(c.Evaluate(MakeDice(3, 3, 3, 5, 5, 5)));
    }

    [Fact]
    public void Level5B_Slot3_TwoThreeOfAKind_Fails_FullHouse()
    {
        // Full house is 3+2, not 3+3, so should fail TwoNOfAKind(3)
        var c = LevelBarConfig.Level5B.SlotCriteria[3];
        Assert.False(c.Evaluate(MakeDice(3, 3, 3, 5, 5)));
    }

    // ----------------------------------------------------------------
    // Level 6 B-side slot correctness
    // ----------------------------------------------------------------

    [Fact]
    public void Level6B_Slot1_Straight6_Passes()
    {
        var c = LevelBarConfig.Level6B.SlotCriteria[1]; // Straight(6)
        Assert.True(c.Evaluate(MakeDice(1, 2, 3, 4, 5, 6)));
    }

    [Fact]
    public void Level6B_Slot1_Straight6_Fails()
    {
        var c = LevelBarConfig.Level6B.SlotCriteria[1];
        Assert.False(c.Evaluate(MakeDice(1, 2, 3, 4, 5, 5)));
    }

    [Fact]
    public void Level6B_Slot2_FourOfAKindAndThreeThrees_Passes()
    {
        // CompoundCriteria(NOfAKind(4), NOfValue(3,3)): need 4-of-a-kind AND three 3s
        // e.g., 3,3,3,3 (four 3s satisfies both NOfAKind(4) and NOfValue(3,3))
        var c = LevelBarConfig.Level6B.SlotCriteria[2];
        Assert.True(c.Evaluate(MakeDice(3, 3, 3, 3, 5, 5)));
    }

    [Fact]
    public void Level6B_Slot2_FourOfAKindAndThreeThrees_Fails_NoThreeThrees()
    {
        var c = LevelBarConfig.Level6B.SlotCriteria[2];
        Assert.False(c.Evaluate(MakeDice(5, 5, 5, 5, 3, 3))); // four 5s but only two 3s
    }

    [Fact]
    public void Level6B_Slot3_PairAndFourOfAKind_Passes()
    {
        // CompoundCriteria(NOfAKind(2), NOfAKind(4)): 4-of-a-kind satisfies both
        var c = LevelBarConfig.Level6B.SlotCriteria[3];
        Assert.True(c.Evaluate(MakeDice(4, 4, 4, 4, 2, 2)));
    }

    [Fact]
    public void Level6B_Slot3_PairAndFourOfAKind_Fails_OnlyThreeOfAKind()
    {
        var c = LevelBarConfig.Level6B.SlotCriteria[3];
        Assert.False(c.Evaluate(MakeDice(4, 4, 4, 2, 2, 1)));
    }

    // ----------------------------------------------------------------
    // Level 7 B-side slot correctness
    // ----------------------------------------------------------------

    [Fact]
    public void Level7B_Slot0_SumGe43_Passes()
    {
        var c = LevelBarConfig.Level7B.SlotCriteria[0]; // SumGreaterEqual(43)
        // 8 dice: 6*7 + 1 = 43
        Assert.True(c.Evaluate(MakeDice(6, 6, 6, 6, 6, 6, 6, 1))); // sum=43
    }

    [Fact]
    public void Level7B_Slot0_SumGe43_Exactly43_Passes()
    {
        var c = LevelBarConfig.Level7B.SlotCriteria[0];
        // e.g., 7 dice: 6+6+6+6+6+6+7... values only go 1-6, so need 8 dice: 6*7+1=43
        Assert.True(c.Evaluate(MakeDice(6, 6, 6, 6, 6, 6, 6, 1))); // sum=43
    }

    [Fact]
    public void Level7B_Slot0_SumGe43_Fails_Sum42()
    {
        var c = LevelBarConfig.Level7B.SlotCriteria[0];
        Assert.False(c.Evaluate(MakeDice(6, 6, 6, 6, 6, 6, 6))); // sum=42
    }

    [Fact]
    public void Level7B_IsHarderThan_Level7A_Queen()
    {
        // B-side threshold (43) is higher than A-side (40)
        var aSlot = LevelBarConfig.Level7A.SlotCriteria[0];
        var bSlot = LevelBarConfig.Level7B.SlotCriteria[0];

        // Sum=41: passes A but not B
        var dice41 = MakeDice(6, 6, 6, 6, 6, 6, 5); // sum=41
        Assert.True(aSlot.Evaluate(dice41));
        Assert.False(bSlot.Evaluate(dice41));

        // Sum=43: passes both
        var dice43 = MakeDice(6, 6, 6, 6, 6, 6, 6, 1); // sum=43
        Assert.True(aSlot.Evaluate(dice43));
        Assert.True(bSlot.Evaluate(dice43));
    }

    // ----------------------------------------------------------------
    // B-side differs from A-side at same level
    // ----------------------------------------------------------------

    [Fact]
    public void Level3_BSide_Slot1_DiffersFrom_ASide()
    {
        // A-side slot 1 = NOfAKind(2) (Pair); B-side slot 1 = AllDiceInRange(1,4)
        var aC = LevelBarConfig.Level3A.SlotCriteria[1];
        var bC = LevelBarConfig.Level3B.SlotCriteria[1];

        var pairOf5s = MakeDice(5, 5, 3); // pair of 5s: passes A (pair), fails B (5 out of range 1-4)
        Assert.True(aC.Evaluate(pairOf5s));
        Assert.False(bC.Evaluate(pairOf5s));
    }

    [Fact]
    public void Level5_BSide_Slot3_DiffersFrom_ASide()
    {
        // A-side slot 3 = FullHouse; B-side slot 3 = TwoNOfAKind(3)
        var aC = LevelBarConfig.Level5A.SlotCriteria[3];
        var bC = LevelBarConfig.Level5B.SlotCriteria[3];

        // Full house (3+2): passes A, fails B
        var fullHouse = MakeDice(3, 3, 3, 5, 5);
        Assert.True(aC.Evaluate(fullHouse));
        Assert.False(bC.Evaluate(fullHouse));

        // Two three-of-a-kinds (3+3): A-side FullHouse requires exactly 3+2 pattern
        var twoTriples = MakeDice(3, 3, 3, 5, 5, 5);
        Assert.True(bC.Evaluate(twoTriples));
    }

    // ----------------------------------------------------------------
    // Game simulation stress tests with B-side
    // ----------------------------------------------------------------

    [Fact]
    public void BSide_TwoPlayer_Game_Completes()
    {
        var rng = new Random(42);
        var state = new GameState(rng);
        state.Setup(
            new List<(string, IPlayerInput)>
            {
                ("Alice", new RandomAIInput(rng)),
                ("Bob",   new RandomAIInput(rng))
            },
            barSide: BarSide.B);

        Assert.Equal(BarSide.B, state.ActiveBarSide);

        var runner = new GameRunner(state);
        var winner = runner.RunGame();

        Assert.NotNull(winner);
    }

    [Fact]
    public void BSide_Fifty_TwoPlayer_Games_NoExceptions()
    {
        var failures = new List<(int seed, string message)>();

        for (int i = 0; i < 50; i++)
        {
            try
            {
                var rng = new Random(i + 5000);
                var state = new GameState(rng);
                state.Setup(
                    new List<(string, IPlayerInput)>
                    {
                        ("P1", new RandomAIInput(rng)),
                        ("P2", new RandomAIInput(rng))
                    },
                    barSide: BarSide.B);

                var runner = new GameRunner(state);
                var winner = runner.RunGame();
                Assert.NotNull(winner);
            }
            catch (Exception ex)
            {
                failures.Add((i + 5000, ex.Message));
            }
        }

        Assert.True(failures.Count == 0,
            $"{failures.Count} B-side 2-player game(s) failed:\n" +
            string.Join("\n", failures.Select(f => $"  seed={f.seed}: {f.message}")));
    }

    [Fact]
    public void BSide_Fifty_ThreePlayer_Games_NoExceptions()
    {
        var failures = new List<(int seed, string message)>();

        for (int i = 0; i < 50; i++)
        {
            try
            {
                var rng = new Random(i + 6000);
                var state = new GameState(rng);
                state.Setup(
                    new List<(string, IPlayerInput)>
                    {
                        ("P1", new RandomAIInput(rng)),
                        ("P2", new RandomAIInput(rng)),
                        ("P3", new RandomAIInput(rng))
                    },
                    barSide: BarSide.B);

                var runner = new GameRunner(state);
                var winner = runner.RunGame();
                Assert.NotNull(winner);
            }
            catch (Exception ex)
            {
                failures.Add((i + 6000, ex.Message));
            }
        }

        Assert.True(failures.Count == 0,
            $"{failures.Count} B-side 3-player game(s) failed:\n" +
            string.Join("\n", failures.Select(f => $"  seed={f.seed}: {f.message}")));
    }

    [Fact]
    public void BSide_Fifty_FourPlayer_Games_NoExceptions()
    {
        var failures = new List<(int seed, string message)>();

        for (int i = 0; i < 50; i++)
        {
            try
            {
                var rng = new Random(i + 7000);
                var state = new GameState(rng);
                state.Setup(
                    new List<(string, IPlayerInput)>
                    {
                        ("P1", new RandomAIInput(rng)),
                        ("P2", new RandomAIInput(rng)),
                        ("P3", new RandomAIInput(rng)),
                        ("P4", new RandomAIInput(rng))
                    },
                    barSide: BarSide.B);

                var runner = new GameRunner(state);
                var winner = runner.RunGame();
                Assert.NotNull(winner);
            }
            catch (Exception ex)
            {
                failures.Add((i + 7000, ex.Message));
            }
        }

        Assert.True(failures.Count == 0,
            $"{failures.Count} B-side 4-player game(s) failed:\n" +
            string.Join("\n", failures.Select(f => $"  seed={f.seed}: {f.message}")));
    }

    [Fact]
    public void BSide_MixedPlayerCount_Hundred_Games_NoExceptions()
    {
        var failures = new List<(int seed, string message)>();

        for (int i = 0; i < 100; i++)
        {
            try
            {
                var rng = new Random(i + 8000);
                int playerCount = (i % 3) + 2; // 2, 3, or 4 players
                var state = new GameState(rng);
                var players = new List<(string, IPlayerInput)>();
                for (int p = 0; p < playerCount; p++)
                    players.Add(($"P{p + 1}", new RandomAIInput(rng)));
                state.Setup(players, barSide: BarSide.B);

                var runner = new GameRunner(state);
                var winner = runner.RunGame();
                Assert.NotNull(winner);
            }
            catch (Exception ex)
            {
                failures.Add((i + 8000, ex.Message));
            }
        }

        Assert.True(failures.Count == 0,
            $"{failures.Count} B-side mixed-player game(s) failed:\n" +
            string.Join("\n", failures.Select(f => $"  seed={f.seed}: {f.message}")));
    }

    // ----------------------------------------------------------------
    // ActiveBarSide is set correctly at setup time
    // ----------------------------------------------------------------

    [Fact]
    public void Setup_ASide_SetsActiveBarSide_A()
    {
        var rng = new Random(1);
        var state = new GameState(rng);
        state.Setup(
            new List<(string, IPlayerInput)> { ("P1", new RandomAIInput(rng)), ("P2", new RandomAIInput(rng)) },
            barSide: BarSide.A);
        Assert.Equal(BarSide.A, state.ActiveBarSide);
    }

    [Fact]
    public void Setup_BSide_SetsActiveBarSide_B()
    {
        var rng = new Random(1);
        var state = new GameState(rng);
        state.Setup(
            new List<(string, IPlayerInput)> { ("P1", new RandomAIInput(rng)), ("P2", new RandomAIInput(rng)) },
            barSide: BarSide.B);
        Assert.Equal(BarSide.B, state.ActiveBarSide);
    }
}
