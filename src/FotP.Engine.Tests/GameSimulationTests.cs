using System;
using System.Collections.Generic;
using FotP.Engine.Players;
using FotP.Engine.State;
using Xunit;

namespace FotP.Engine.Tests
{
    public class GameSimulationTests
    {
        [Fact]
        public void Two_Player_Game_Completes_Without_Exception()
        {
            var rng = new Random(42);
            var state = new GameState(rng);
            state.Setup(new List<(string, IPlayerInput)>
            {
                ("Alice", new RandomAIInput(rng)),
                ("Bob", new RandomAIInput(rng))
            });

            var runner = new GameRunner(state);
            var winner = runner.RunGame();

            Assert.NotNull(winner);
            Assert.True(state.Phase == GamePhase.GameOver || state.RoundNumber > 50);
        }

        [Fact]
        public void Hundred_AI_Games_No_Exceptions()
        {
            int errors = 0;
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    var rng = new Random(i);
                    var state = new GameState(rng);
                    state.Setup(new List<(string, IPlayerInput)>
                    {
                        ("P1", new RandomAIInput(rng)),
                        ("P2", new RandomAIInput(rng))
                    });

                    var runner = new GameRunner(state);
                    var winner = runner.RunGame();
                    Assert.NotNull(winner);
                }
                catch (Exception)
                {
                    errors++;
                }
            }

            Assert.Equal(0, errors);
        }

        [Fact]
        public void Three_Player_Game_Completes()
        {
            var rng = new Random(123);
            var state = new GameState(rng);
            state.Setup(new List<(string, IPlayerInput)>
            {
                ("Alice", new RandomAIInput(rng)),
                ("Bob", new RandomAIInput(rng)),
                ("Charlie", new RandomAIInput(rng))
            });

            var runner = new GameRunner(state);
            var winner = runner.RunGame();

            Assert.NotNull(winner);
        }
    }
}
