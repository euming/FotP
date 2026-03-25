using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;
using Xunit;

namespace FotP.Engine.Tests
{
    public class StateMachineTests
    {
        private GameState CreateTestState(int seed = 42)
        {
            var rng = new Random(seed);
            var state = new GameState(rng);
            var ai = new RandomAIInput(rng);
            state.Setup(new List<(string, IPlayerInput)>
            {
                ("Alice", ai),
                ("Bob", ai)
            });
            return state;
        }

        [Fact]
        public void BeginTurn_Sets_Phase_To_StartOfTurn()
        {
            var state = CreateTestState();
            state.StartTurn();
            Assert.Equal(TurnPhase.StartOfTurn, state.TurnState.Phase);
        }

        [Fact]
        public void PerformRoll_Moves_To_Locking_Phase()
        {
            var state = CreateTestState();
            state.StartTurn();
            state.TurnState.PerformRoll(state);

            // Phase should be Locking after roll (auto-lock immediate dice happens first)
            Assert.Equal(TurnPhase.Locking, state.TurnState.Phase);
        }

        [Fact]
        public void PerformRoll_Increments_RollCount()
        {
            var state = CreateTestState();
            state.StartTurn();
            Assert.Equal(0, state.TurnState.RollCount);

            state.TurnState.PerformRoll(state);
            Assert.Equal(1, state.TurnState.RollCount);
        }

        [Fact]
        public void LockDice_Requires_At_Least_One()
        {
            var state = CreateTestState();
            state.StartTurn();
            state.TurnState.PerformRoll(state);

            Assert.Throws<InvalidOperationException>(() =>
                state.TurnState.LockDice(new List<Die>(), state));
        }

        [Fact]
        public void LockDice_Moves_Die_From_Active_To_Locked()
        {
            var state = CreateTestState();
            state.StartTurn();
            state.TurnState.PerformRoll(state);

            var activeDie = state.TurnState.Zones.Active[0];
            state.TurnState.LockDice(new List<Die> { activeDie }, state);

            Assert.True(activeDie.IsLocked);
            Assert.Contains(activeDie, state.TurnState.Zones.Locked);
            Assert.DoesNotContain(activeDie, state.TurnState.Zones.Active);
        }

        [Fact]
        public void Cannot_Roll_During_Locking_Phase()
        {
            var state = CreateTestState();
            state.StartTurn();
            state.TurnState.PerformRoll(state);

            // Phase is Locking, cannot roll
            Assert.Throws<InvalidOperationException>(() =>
                state.TurnState.PerformRoll(state));
        }

        [Fact]
        public void Full_Turn_Flow()
        {
            var state = CreateTestState();
            state.StartTurn();

            // Roll
            state.TurnState.PerformRoll(state);
            Assert.True(state.TurnState.Zones.Active.Count > 0);

            // Lock one die
            var die = state.TurnState.Zones.Active[0];
            state.TurnState.LockDice(new List<Die> { die }, state);

            // Should be in ScarabUse (if not all dice locked)
            if (state.TurnState.Phase == TurnPhase.ScarabUse)
            {
                state.TurnState.FinishScarabPhase();
                Assert.Equal(TurnPhase.ContinueDecision, state.TurnState.Phase);

                // Decide to claim
                state.TurnState.DecideToClaim();
                Assert.Equal(TurnPhase.Claiming, state.TurnState.Phase);
            }

            // Claim nothing
            state.TurnState.ClaimTile(null, state.CurrentPlayer!, state);
            Assert.Equal(TurnPhase.PostClaim, state.TurnState.Phase);

            // End turn
            state.TurnState.EndTurn(state);
            Assert.Equal(TurnPhase.EndOfTurn, state.TurnState.Phase);
        }

        [Fact]
        public void DiceZone_CollectAll_Removes_Temporary()
        {
            var state = CreateTestState();
            var player = state.CurrentPlayer!;

            // Add a temp die
            var tempDie = new Die(DieType.Standard) { IsTemporary = true };
            player.DicePool.Add(tempDie);
            state.TurnState.Zones.Temporary.Add(tempDie);

            int originalCount = player.DicePool.Count;

            state.TurnState.Zones.CollectAllToCup(player.DicePool);

            // Temp die should be removed
            Assert.DoesNotContain(tempDie, player.DicePool);
            Assert.Equal(originalCount - 1, player.DicePool.Count);
        }
    }
}
