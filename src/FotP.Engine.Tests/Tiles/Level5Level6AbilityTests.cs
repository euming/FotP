using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;
using FotP.Engine.Tests.Helpers;
using FotP.Engine.Tiles;
using FotP.Engine.Tiles.Abilities;
using Xunit;

namespace FotP.Engine.Tests.Tiles;

/// <summary>
/// Spot-check tests for 10 L5/L6 abilities against the FotP rules.
/// </summary>
public class Level5Level6AbilityTests
{
    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static (GameState state, Player player) MakeStateWithPlayer(IPlayerInput? input = null)
    {
        var state = new GameState(new Random(42));
        state.Setup(new List<(string, IPlayerInput)>
        {
            ("Alice", input ?? new ScriptedPlayerInput(alwaysStop: true, neverUseAbility: false)),
            ("Bob",   new ScriptedPlayerInput())
        });
        return (state, state.TurnOrder[0]);
    }

    private static Die AddActiveDie(GameState state, Player player, int value = 3)
    {
        var die = new Die(DieType.Standard);
        die.SetValue(value);
        player.DicePool.Add(die);
        state.TurnState.Zones.Active.Add(die);
        return die;
    }

    private static Die AddLockedDie(GameState state, Player player, int value = 3)
    {
        var die = new Die(DieType.Standard);
        die.SetValue(value);
        die.IsLocked = true;
        player.DicePool.Add(die);
        state.TurnState.Zones.Locked.Add(die);
        return die;
    }

    // ─── Priest (Blue L5) ────────────────────────────────────────────────────

    [Fact]
    public void Priest_AfterRoll_RerollsOneDieAndBoostsAnother()
    {
        var (state, player) = MakeStateWithPlayer();
        var ability = new PriestAbility();

        // Two active dice: die1 will be rerolled, die2 at value 2 will get +1 pip
        // ScriptedPlayerInput.ChooseDie returns first → die1 gets rerolled
        var die1 = AddActiveDie(state, player, 4);
        var die2 = AddActiveDie(state, player, 2);

        int die2PipBefore = die2.TempPipModifier;
        ability.Execute(state, player);

        // die2 should have gotten +1 pip boost
        Assert.Equal(die2PipBefore + 1, die2.TempPipModifier);
    }

    [Fact]
    public void Priest_TriggerType_IsAfterRoll()
    {
        Assert.Equal(TriggerType.AfterRoll, new PriestAbility().TriggerType);
    }

    [Fact]
    public void Priest_IsPerTurn()
    {
        Assert.True(new PriestAbility().IsPerTurn);
    }

    // ─── Priestess (Yellow L6) ───────────────────────────────────────────────

    [Fact]
    public void Priestess_AfterRoll_BoostsOneDie()
    {
        var (state, player) = MakeStateWithPlayer();
        var ability = new PriestessAbility();

        var die = AddActiveDie(state, player, 2);
        int modBefore = die.TempPipModifier;
        ability.Execute(state, player);

        // +1 pip should have been applied to a die
        Assert.Equal(modBefore + 1, die.TempPipModifier);
    }

    [Fact]
    public void Priestess_IsPerTurn()
    {
        Assert.True(new PriestessAbility().IsPerTurn);
    }

    // ─── Embalmer (Yellow L6) ────────────────────────────────────────────────

    [Fact]
    public void Embalmer_AfterRoll_AddsNewStandardDieAtSix()
    {
        var (state, player) = MakeStateWithPlayer();
        var ability = new EmbalmerAbility();

        int activeBefore = state.TurnState.Zones.Active.Count;
        ability.Execute(state, player);
        int activeAfter = state.TurnState.Zones.Active.Count;

        Assert.Equal(activeBefore + 1, activeAfter);

        var newDie = state.TurnState.Zones.Active.Last();
        Assert.Equal(DieType.Standard, newDie.DieType);
        Assert.Equal(6, newDie.PipValue);
        Assert.True(newDie.IsTemporary);
    }

    [Fact]
    public void Embalmer_IsPerTurn()
    {
        Assert.True(new EmbalmerAbility().IsPerTurn);
    }

    // ─── Charioteer (Yellow L5) ──────────────────────────────────────────────

    [Fact]
    public void Charioteer_StartOfTurn_AddsTwoStandardDiceToCup()
    {
        var (state, player) = MakeStateWithPlayer();
        var ability = new CharioteerAbility();

        state.TurnState.BeginTurn(player, state);
        int cupBefore = state.TurnState.Zones.Cup.Count;
        ability.Execute(state, player);
        int cupAfter = state.TurnState.Zones.Cup.Count;

        Assert.Equal(cupBefore + 2, cupAfter);
        Assert.Equal(2, state.TurnState.Zones.Temporary.Count);
    }

    // ─── Bad Omen (Red L5) ───────────────────────────────────────────────────

    [Fact]
    public void BadOmen_EndOfTurn_OtherPlayerGetsMinusTwoDiceModifier()
    {
        var (state, player) = MakeStateWithPlayer();
        var bob = state.TurnOrder[1];
        var ability = new BadOmenAbility();

        ability.Execute(state, player);

        Assert.Equal(-2, bob.StandardDiceModifierNextTurn);
    }

    [Fact]
    public void BadOmen_EndOfTurn_ActivePlayerGetsPlusOneDiceModifier()
    {
        var (state, player) = MakeStateWithPlayer();
        var ability = new BadOmenAbility();

        ability.Execute(state, player);

        Assert.Equal(1, player.StandardDiceModifierNextTurn);
    }

    [Fact]
    public void BadOmen_DiceModifier_AppliedAtBeginTurn()
    {
        var (state, player) = MakeStateWithPlayer();
        var bob = state.TurnOrder[1];

        // Give bob 4 standard dice
        bob.DicePool.Clear();
        for (int i = 0; i < 4; i++)
            bob.DicePool.Add(new Die(DieType.Standard));

        // Apply -2 modifier
        bob.StandardDiceModifierNextTurn = -2;
        state.TurnState.BeginTurn(bob, state);

        // Should have 2 dice in cup (4 - 2 = 2)
        Assert.Equal(2, state.TurnState.Zones.Cup.Count);
    }

    // ─── Tomb Builder (Blue L5) ──────────────────────────────────────────────

    [Fact]
    public void TombBuilder_AfterRoll_LocksChosenDieAtChosenValue()
    {
        // ScriptedPlayerInput.ChoosePipValue returns 3; ChooseDie returns first die
        var (state, player) = MakeStateWithPlayer();
        var ability = new TombBuilderAbility();

        var die = AddActiveDie(state, player, 5);
        ability.Execute(state, player);

        // Die should be locked and in Locked zone
        Assert.True(die.IsLocked);
        Assert.Contains(die, state.TurnState.Zones.Locked);
        Assert.DoesNotContain(die, state.TurnState.Zones.Active);
        // Value set to 3 by ScriptedPlayerInput.ChoosePipValue
        Assert.Equal(3, die.PipValue);
    }

    [Fact]
    public void TombBuilder_IsPerRoll()
    {
        Assert.True(new TombBuilderAbility().IsPerRoll);
    }

    // ─── Head Servant (Blue L5) ──────────────────────────────────────────────

    [Fact]
    public void HeadServant_AfterRoll_CanRerollMultipleDice()
    {
        // ScriptedPlayerInput.ChooseMultipleDice returns first die only
        var (state, player) = MakeStateWithPlayer();
        var ability = new HeadServantAbility();

        AddActiveDie(state, player, 2);
        AddActiveDie(state, player, 4);

        // Should not throw - just rerolls the chosen subset
        ability.Execute(state, player);
    }

    [Fact]
    public void HeadServant_IsPerRoll()
    {
        Assert.True(new HeadServantAbility().IsPerRoll);
    }

    // ─── Astrologer (Blue L6) ────────────────────────────────────────────────

    [Fact]
    public void Astrologer_IsPerTurn_NotPerRoll()
    {
        var ability = new AstrologerAbility();
        Assert.True(ability.IsPerTurn);
        Assert.False(ability.IsPerRoll);
    }

    [Fact]
    public void Astrologer_AfterRoll_SetsDieToChosenValue()
    {
        // ScriptedPlayerInput.ChoosePipValue returns 3
        var (state, player) = MakeStateWithPlayer();
        var ability = new AstrologerAbility();

        var die = AddActiveDie(state, player, 5);
        ability.Execute(state, player);

        Assert.Equal(3, die.PipValue);
    }

    [Fact]
    public void Astrologer_CannotFireTwiceInSameTurn()
    {
        var (state, player) = MakeStateWithPlayer();
        var ability = new AstrologerAbility();
        AddActiveDie(state, player);

        Assert.True(ability.CanActivate(state, player));
        ability.IsUsedThisTurn = true;
        Assert.False(ability.CanActivate(state, player));
    }

    // ─── Grain Trader (Blue L6) ──────────────────────────────────────────────

    [Fact]
    public void GrainTrader_AfterRoll_AddsPipToChosenDie()
    {
        var (state, player) = MakeStateWithPlayer();
        var ability = new GrainTraderAbility();

        var die = AddActiveDie(state, player, 2);
        int modBefore = die.TempPipModifier;
        ability.Execute(state, player);

        Assert.Equal(modBefore + 1, die.TempPipModifier);
    }

    [Fact]
    public void GrainTrader_TriggerType_IsAfterRoll()
    {
        Assert.Equal(TriggerType.AfterRoll, new GrainTraderAbility().TriggerType);
    }

    [Fact]
    public void GrainTrader_HasNoPerTurnOrPerRollLimit()
    {
        var ability = new GrainTraderAbility();
        Assert.False(ability.IsPerTurn);
        Assert.False(ability.IsPerRoll);
    }

    // ─── Ship Captain (Yellow L5) ────────────────────────────────────────────

    [Fact]
    public void ShipCaptain_StartOfTurn_AddsVoyageDieToCup()
    {
        var (state, player) = MakeStateWithPlayer();
        var ability = new ShipCaptainAbility();

        state.TurnState.BeginTurn(player, state);
        int cupBefore = state.TurnState.Zones.Cup.Count;
        ability.Execute(state, player);
        int cupAfter = state.TurnState.Zones.Cup.Count;

        Assert.Equal(cupBefore + 1, cupAfter);

        var voyageDie = state.TurnState.Zones.Cup.Last();
        Assert.Equal(DieType.Voyage, voyageDie.DieType);
        Assert.True(voyageDie.IsTemporary);
    }
}
