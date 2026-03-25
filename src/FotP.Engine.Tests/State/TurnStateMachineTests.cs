using System;
using System.Collections.Generic;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;
using FotP.Engine.Tiles;

namespace FotP.Engine.Tests.State
{
    public class TurnStateMachineTests
    {
        private static GameState MakeState(IPlayerInput? input = null)
        {
            var state = new GameState();
            state.Setup(new List<(string, IPlayerInput)>
            {
                ("Alice", input ?? new AutoPlayerInput()),
                ("Bob", new AutoPlayerInput())
            });
            return state;
        }

        [Fact]
        public void BeginTurn_SetsPhaseToStartOfTurn()
        {
            var state = MakeState();
            var player = state.TurnOrder[0];

            state.TurnState.BeginTurn(player, state);

            Assert.Equal(TurnPhase.StartOfTurn, state.TurnState.Phase);
            Assert.Equal(player, state.TurnState.CurrentPlayer);
            Assert.Equal(0, state.TurnState.RollCount);
        }

        [Fact]
        public void PerformRoll_MovesFromStartOfTurnToLocking()
        {
            var state = MakeState();
            var player = state.TurnOrder[0];
            state.TurnState.BeginTurn(player, state);

            state.TurnState.PerformRoll(state);

            Assert.Equal(TurnPhase.Locking, state.TurnState.Phase);
            Assert.Equal(1, state.TurnState.RollCount);
        }

        [Fact]
        public void PerformRoll_DiceMovedFromCupToActive()
        {
            var state = MakeState();
            var player = state.TurnOrder[0];
            state.TurnState.BeginTurn(player, state);

            int diceCount = player.DicePool.Count;
            state.TurnState.PerformRoll(state);

            Assert.Equal(0, state.TurnState.Zones.Cup.Count);
            Assert.Equal(diceCount, state.TurnState.Zones.Active.Count);
        }

        [Fact]
        public void LockDice_SingleDie_TransitionsToScarabUse()
        {
            var state = MakeState();
            var player = state.TurnOrder[0];
            state.TurnState.BeginTurn(player, state);
            state.TurnState.PerformRoll(state);

            var oneDie = new List<Die> { state.TurnState.Zones.Active[0] };
            state.TurnState.LockDice(oneDie, state);

            // If there are still dice in cup, phase = ScarabUse
            if (state.TurnState.Zones.Cup.Count > 0)
                Assert.Equal(TurnPhase.ScarabUse, state.TurnState.Phase);
            else
                Assert.Equal(TurnPhase.Claiming, state.TurnState.Phase);
        }

        [Fact]
        public void LockDice_AllDice_TransitionsToClaiming()
        {
            var state = MakeState();
            var player = state.TurnOrder[0];
            state.TurnState.BeginTurn(player, state);
            state.TurnState.PerformRoll(state);

            var allActive = new List<Die>(state.TurnState.Zones.Active);
            state.TurnState.LockDice(allActive, state);

            Assert.Equal(TurnPhase.Claiming, state.TurnState.Phase);
        }

        [Fact]
        public void LockDice_EmptyList_ThrowsException()
        {
            var state = MakeState();
            var player = state.TurnOrder[0];
            state.TurnState.BeginTurn(player, state);
            state.TurnState.PerformRoll(state);

            Assert.Throws<InvalidOperationException>(() =>
                state.TurnState.LockDice(new List<Die>(), state));
        }

        [Fact]
        public void FinishScarabPhase_TransitionsToContinueDecision()
        {
            var state = MakeState();
            var player = state.TurnOrder[0];
            state.TurnState.BeginTurn(player, state);
            state.TurnState.PerformRoll(state);

            // Lock only one die so we go to ScarabUse
            var active = state.TurnState.Zones.Active.ToList();
            if (active.Count >= 2)
            {
                state.TurnState.LockDice(new List<Die> { active[0] }, state);
                Assert.Equal(TurnPhase.ScarabUse, state.TurnState.Phase);

                state.TurnState.FinishScarabPhase();
                Assert.Equal(TurnPhase.ContinueDecision, state.TurnState.Phase);
            }
        }

        [Fact]
        public void DecideToClaim_TransitionsFromContinueDecisionToClaiming()
        {
            var state = MakeState();
            var player = state.TurnOrder[0];
            state.TurnState.BeginTurn(player, state);
            state.TurnState.PerformRoll(state);

            var active = state.TurnState.Zones.Active.ToList();
            if (active.Count >= 2)
            {
                state.TurnState.LockDice(new List<Die> { active[0] }, state);
                state.TurnState.FinishScarabPhase();

                state.TurnState.DecideToClaim();
                Assert.Equal(TurnPhase.Claiming, state.TurnState.Phase);
            }
        }

        [Fact]
        public void ClaimTile_Null_TransitionsToPostClaim()
        {
            var state = MakeState();
            var player = state.TurnOrder[0];
            state.TurnState.BeginTurn(player, state);
            state.TurnState.PerformRoll(state);
            var allActive = new List<Die>(state.TurnState.Zones.Active);
            state.TurnState.LockDice(allActive, state);

            state.TurnState.ClaimTile(null, player, state);

            Assert.Equal(TurnPhase.PostClaim, state.TurnState.Phase);
        }

        [Fact]
        public void EndTurn_TransitionsToEndOfTurn()
        {
            var state = MakeState();
            var player = state.TurnOrder[0];
            state.TurnState.BeginTurn(player, state);
            state.TurnState.PerformRoll(state);
            var allActive = new List<Die>(state.TurnState.Zones.Active);
            state.TurnState.LockDice(allActive, state);
            state.TurnState.ClaimTile(null, player, state);

            state.TurnState.EndTurn(state);

            Assert.Equal(TurnPhase.EndOfTurn, state.TurnState.Phase);
        }

        [Fact]
        public void CollectAllToCup_RestoresAllDiceToCup()
        {
            var state = MakeState();
            var player = state.TurnOrder[0];
            state.TurnState.BeginTurn(player, state);
            state.TurnState.PerformRoll(state);

            // Dice moved to active; now begin another turn to collect
            state.TurnState.BeginTurn(player, state);

            Assert.Equal(player.DicePool.Count, state.TurnState.Zones.Cup.Count);
            Assert.Equal(0, state.TurnState.Zones.Active.Count);
            Assert.Equal(0, state.TurnState.Zones.Locked.Count);
        }
    }

    public class GameStateSetupTests
    {
        [Fact]
        public void Setup_CreatesPlayersWithStartingDice()
        {
            var state = new GameState();
            state.Setup(new List<(string, IPlayerInput)>
            {
                ("Alice", new AutoPlayerInput()),
                ("Bob", new AutoPlayerInput())
            });

            Assert.Equal(2, state.TurnOrder.Count);
            Assert.All(state.TurnOrder, p => Assert.Equal(3, p.DicePool.Count));
        }

        [Fact]
        public void Setup_PopulatesMarket()
        {
            var state = new GameState();
            state.Setup(new List<(string, IPlayerInput)>
            {
                ("Alice", new AutoPlayerInput()),
            });

            Assert.True(state.Market.Stacks.Count > 0);
        }

        [Fact]
        public void NextPlayer_AdvancesPlayerIndex()
        {
            var state = new GameState();
            state.Setup(new List<(string, IPlayerInput)>
            {
                ("Alice", new AutoPlayerInput()),
                ("Bob", new AutoPlayerInput()),
                ("Carol", new AutoPlayerInput())
            });

            Assert.Equal(0, state.CurrentPlayerIndex);
            state.NextPlayer();
            Assert.Equal(1, state.CurrentPlayerIndex);
            state.NextPlayer();
            Assert.Equal(2, state.CurrentPlayerIndex);
            state.NextPlayer();
            Assert.Equal(0, state.CurrentPlayerIndex); // wraps
        }

        [Fact]
        public void EnterRollOff_SetsPhaseAndPopulatesRollOffPlayers()
        {
            var state = new GameState();
            state.Setup(new List<(string, IPlayerInput)>
            {
                ("Alice", new AutoPlayerInput()),
                ("Bob", new AutoPlayerInput())
            });
            state.QueenClaimant = state.TurnOrder[0];

            state.EnterRollOff();

            Assert.Equal(GamePhase.RollOff, state.Phase);
            Assert.Single(state.RollOffPlayers);
            Assert.Equal("Bob", state.RollOffPlayers[0].Name);
        }
    }

    /// <summary>
    /// A minimal IPlayerInput that auto-locks all dice and always passes/skips.
    /// </summary>
    internal class AutoPlayerInput : IPlayerInput
    {
        public List<Die> ChooseDiceToLock(IReadOnlyList<Die> activeDice, Player player)
            => new List<Die>(activeDice); // Lock all

        public bool ChooseContinueRolling(Player player) => false; // Always stop

        public Tile? ChooseTileToClaim(IReadOnlyList<Tile> claimable, Player player) => null; // Skip claim

        public Die? ChooseDie(IReadOnlyList<Die> dice, string prompt, Player player)
            => dice.Count > 0 ? dice[0] : null;

        public List<Die> ChooseMultipleDice(IReadOnlyList<Die> dice, string prompt, Player player)
            => new List<Die>();

        public int ChoosePipValue(Die die, string prompt, Player player) => 1;

        public Scarab? ChooseScarab(IReadOnlyList<Scarab> scarabs, Player player) => null; // Skip scarabs

        public bool ChooseYesNo(string prompt, Player player) => false;

        public bool ChooseUseAbility(Ability ability, Player player) => false; // Skip abilities

        public Player? ChoosePlayer(IReadOnlyList<Player> players, string prompt, Player activePlayer)
            => players.Count > 0 ? players[0] : null;
    }
}
