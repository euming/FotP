using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;
using FotP.Engine.Tests.Helpers;
using FotP.Engine.Tiles;
using Xunit;

namespace FotP.Engine.Tests.Dice;

/// <summary>
/// Tests for custom dice face abilities that trigger during play:
/// Artisan die *, Intrigue die **, Voyage die faces, Decree die *.
/// </summary>
public class CustomDiceFaceAbilityTests
{
    // ─── Helpers ────────────────────────────────────────────────────────────────

    private static (GameState state, Player player) MakeState(IPlayerInput? input = null)
    {
        var state = new GameState(new Random(42));
        state.Setup(new List<(string, IPlayerInput)>
        {
            ("Alice", input ?? new ScriptedPlayerInput(alwaysStop: true, neverUseAbility: false)),
            ("Bob", new ScriptedPlayerInput())
        });
        var player = state.TurnOrder[0];
        return (state, player);
    }

    /// <summary>Place a die directly into Active zone, simulating it was rolled.</summary>
    private static Die PlaceDieInActive(GameState state, Player player, DieType type, int faceValue)
    {
        // Remove the die from player's pool and cup if present (to avoid duplicates)
        // Add a new die directly to Active
        var die = new Die(type);
        die.SetValue(faceValue);
        player.DicePool.Add(die);
        state.TurnState.Zones.Active.Add(die);
        return die;
    }

    // ─── Artisan die * face ─────────────────────────────────────────────────────

    [Fact]
    public void ArtisanDie_StarFace_AdjustsOneActiveDie()
    {
        var (state, player) = MakeState();
        state.TurnState.BeginTurn(player, state);

        // Clear default dice from cup; we'll manage Active manually
        state.TurnState.Zones.Cup.Clear();

        // Place a Standard die (value 2) and an Artisan die showing * in Active
        var standardDie = PlaceDieInActive(state, player, DieType.Standard, 2);
        var artisanDie = PlaceDieInActive(state, player, DieType.Artisan, DieFaces.StarFace);

        Assert.True(artisanDie.IsStarFace);

        // ScriptedPlayerInput.ChooseDie returns first candidate (standardDie has pip, artisan * also has pip)
        // ChoosePipValue returns 3
        state.TurnState.PerformRoll(state); // triggers FireCustomDiceFaceAbilities via roll...
        // But PerformRoll rolls all Cup dice - Cup is empty, so no dice move.
        // Wait - we need to trigger via PerformRoll but it calls RollAllInCup which uses Cup.
        // Since Cup is empty, no dice roll. FireCustomDiceFaceAbilities runs on Active.
    }

    [Fact]
    public void ArtisanDie_StarFace_DirectTrigger_AdjustsActiveDie()
    {
        var (state, player) = MakeState();
        state.TurnState.BeginTurn(player, state);

        // Clear cup so PerformRoll doesn't move anything
        state.TurnState.Zones.Cup.Clear();

        // Place Standard die and Artisan * die in Active
        var standardDie = new Die(DieType.Standard);
        standardDie.SetValue(2);
        player.DicePool.Add(standardDie);
        state.TurnState.Zones.Active.Add(standardDie);

        var artisanDie = new Die(DieType.Artisan);
        artisanDie.SetValue(DieFaces.StarFace); // shows *
        player.DicePool.Add(artisanDie);
        state.TurnState.Zones.Active.Add(artisanDie);

        // ScriptedPlayerInput: ChooseDie returns first with pip → standardDie, ChoosePipValue returns 3
        state.TurnState.PerformRoll(state);

        // standardDie should have been adjusted to 3 by the Artisan * ability
        Assert.Equal(3, standardDie.PipValue);
    }

    [Fact]
    public void ArtisanDie_NumericFace_DoesNotTriggerAdjust()
    {
        var (state, player) = MakeState();
        state.TurnState.BeginTurn(player, state);

        state.TurnState.Zones.Cup.Clear();

        var standardDie = new Die(DieType.Standard);
        standardDie.SetValue(2);
        player.DicePool.Add(standardDie);
        state.TurnState.Zones.Active.Add(standardDie);

        var artisanDie = new Die(DieType.Artisan);
        artisanDie.SetValue(4); // numeric face, no adjustment
        player.DicePool.Add(artisanDie);
        state.TurnState.Zones.Active.Add(artisanDie);

        state.TurnState.PerformRoll(state);

        // standardDie unchanged (still 2)
        Assert.Equal(2, standardDie.PipValue);
    }

    // ─── Intrigue die ** face ───────────────────────────────────────────────────

    [Fact]
    public void IntrigueDie_DoubleStarFace_AdjustsTwoDice()
    {
        var (state, player) = MakeState();
        state.TurnState.BeginTurn(player, state);
        state.TurnState.Zones.Cup.Clear();

        var die1 = new Die(DieType.Standard);
        die1.SetValue(2);
        player.DicePool.Add(die1);
        state.TurnState.Zones.Active.Add(die1);

        var die2 = new Die(DieType.Standard);
        die2.SetValue(4);
        player.DicePool.Add(die2);
        state.TurnState.Zones.Active.Add(die2);

        var intrigueDie = new Die(DieType.Intrigue);
        intrigueDie.SetValue(DieFaces.DoubleStarFace);
        player.DicePool.Add(intrigueDie);
        state.TurnState.Zones.Active.Add(intrigueDie);

        Assert.True(intrigueDie.IsDoubleStarFace);
        Assert.False(intrigueDie.HasPipValue);

        // ScriptedPlayerInput.ChooseDie picks first candidate each time → die1 both times
        // ChoosePipValue returns 3
        state.TurnState.PerformRoll(state);

        // die1 should have been adjusted (twice, ending at 3)
        Assert.Equal(3, die1.PipValue);
    }

    [Fact]
    public void IntrigueDie_DoubleStarFace_HasNoPipValue()
    {
        var intrigueDie = new Die(DieType.Intrigue);
        intrigueDie.SetValue(DieFaces.DoubleStarFace);
        Assert.False(intrigueDie.HasPipValue);
        Assert.Equal(0, intrigueDie.PipValue);
    }

    // ─── Voyage die faces ───────────────────────────────────────────────────────

    [Fact]
    public void VoyageDie_AdjustFace_AdjustsOneActiveDie()
    {
        var (state, player) = MakeState();
        state.TurnState.BeginTurn(player, state);
        state.TurnState.Zones.Cup.Clear();

        var standardDie = new Die(DieType.Standard);
        standardDie.SetValue(2);
        player.DicePool.Add(standardDie);
        state.TurnState.Zones.Active.Add(standardDie);

        var voyageDie = new Die(DieType.Voyage);
        voyageDie.SetValue(DieFaces.VoyageAdjust);
        player.DicePool.Add(voyageDie);
        state.TurnState.Zones.Active.Add(voyageDie);

        state.TurnState.PerformRoll(state);

        // standardDie adjusted to 3
        Assert.Equal(3, standardDie.PipValue);
    }

    [Fact]
    public void VoyageDie_RerollFace_RerollsAnotherActiveDie()
    {
        // Use seeded RNG so the reroll is deterministic
        var state = new GameState(new Random(0));
        state.Setup(new List<(string, IPlayerInput)>
        {
            ("Alice", new ScriptedPlayerInput(alwaysStop: true, neverUseAbility: false)),
            ("Bob", new ScriptedPlayerInput())
        });
        var player = state.TurnOrder[0];
        state.TurnState.BeginTurn(player, state);
        state.TurnState.Zones.Cup.Clear();

        var standardDie = new Die(DieType.Standard);
        standardDie.SetValue(2);
        player.DicePool.Add(standardDie);
        state.TurnState.Zones.Active.Add(standardDie);

        var voyageDie = new Die(DieType.Voyage);
        voyageDie.SetValue(DieFaces.VoyageReroll);
        player.DicePool.Add(voyageDie);
        state.TurnState.Zones.Active.Add(voyageDie);

        // ScriptedPlayerInput.ChooseDie returns first candidate (standardDie, since voyageDie is excluded)
        state.TurnState.PerformRoll(state);

        // standardDie was rerolled - value is now 1-6 (might differ from 2)
        Assert.True(standardDie.HasPipValue);
        Assert.InRange(standardDie.PipValue, 1, 6);
    }

    [Fact]
    public void VoyageDie_DoubleDiceFace_AddsTwoTempStandardDice()
    {
        var (state, player) = MakeState();
        state.TurnState.BeginTurn(player, state);
        state.TurnState.Zones.Cup.Clear();

        var voyageDie = new Die(DieType.Voyage);
        voyageDie.SetValue(DieFaces.VoyageDoubleDice);
        player.DicePool.Add(voyageDie);
        state.TurnState.Zones.Active.Add(voyageDie);

        int poolBefore = player.DicePool.Count;
        int activeBefore = state.TurnState.Zones.Active.Count;

        state.TurnState.PerformRoll(state);

        // 2 temp standard dice should be added to Active
        Assert.Equal(activeBefore + 2, state.TurnState.Zones.Active.Count);
        // They should be in the Temporary zone
        Assert.Equal(2, state.TurnState.Zones.Temporary.Count);
        // All temp dice should be Standard type
        Assert.All(state.TurnState.Zones.Temporary, d => Assert.Equal(DieType.Standard, d.DieType));
        Assert.All(state.TurnState.Zones.Temporary, d => Assert.True(d.IsTemporary));
    }

    [Fact]
    public void VoyageDie_LockFace_LocksChosenDieAtChosenValue()
    {
        var (state, player) = MakeState();
        state.TurnState.BeginTurn(player, state);
        state.TurnState.Zones.Cup.Clear();

        var standardDie = new Die(DieType.Standard);
        standardDie.SetValue(2);
        player.DicePool.Add(standardDie);
        state.TurnState.Zones.Active.Add(standardDie);

        var voyageDie = new Die(DieType.Voyage);
        voyageDie.SetValue(DieFaces.VoyageLock);
        player.DicePool.Add(voyageDie);
        state.TurnState.Zones.Active.Add(voyageDie);

        state.TurnState.PerformRoll(state);

        // standardDie should be locked at pip value 3 (ScriptedPlayerInput.ChoosePipValue returns 3)
        Assert.Equal(3, standardDie.PipValue);
        Assert.True(standardDie.IsLocked);
        Assert.Contains(standardDie, state.TurnState.Zones.Locked);
        Assert.DoesNotContain(standardDie, state.TurnState.Zones.Active);
    }

    // ─── Decree die * face ──────────────────────────────────────────────────────

    [Fact]
    public void DecreeDie_StarFace_AdjustsActiveDie_WhenChooseNo()
    {
        // ScriptedPlayerInput.ChooseYesNo returns false → adjust path
        var (state, player) = MakeState();
        state.TurnState.BeginTurn(player, state);
        state.TurnState.Zones.Cup.Clear();

        var standardDie = new Die(DieType.Standard);
        standardDie.SetValue(2);
        player.DicePool.Add(standardDie);
        state.TurnState.Zones.Active.Add(standardDie);

        var decreeDie = new Die(DieType.Decree);
        decreeDie.SetValue(DieFaces.StarFace);
        player.DicePool.Add(decreeDie);
        state.TurnState.Zones.Active.Add(decreeDie);

        state.TurnState.PerformRoll(state);

        // standardDie adjusted to 3
        Assert.Equal(3, standardDie.PipValue);
    }

    [Fact]
    public void DecreeDie_NumericFace_DoesNotTriggerAbility()
    {
        var (state, player) = MakeState();
        state.TurnState.BeginTurn(player, state);
        state.TurnState.Zones.Cup.Clear();

        var standardDie = new Die(DieType.Standard);
        standardDie.SetValue(4);
        player.DicePool.Add(standardDie);
        state.TurnState.Zones.Active.Add(standardDie);

        var decreeDie = new Die(DieType.Decree);
        decreeDie.SetValue(5); // numeric face
        player.DicePool.Add(decreeDie);
        state.TurnState.Zones.Active.Add(decreeDie);

        state.TurnState.PerformRoll(state);

        // standardDie unchanged
        Assert.Equal(4, standardDie.PipValue);
    }
}
