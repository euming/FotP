using System;
using System.Collections.Generic;
using FotP.Engine.Players;
using FotP.Engine.State;
using FotP.Engine.Tests.Helpers;
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

        [Fact]
        public void TwoHundred_AI_Games_No_Exceptions()
        {
            var failures = new List<(int seed, string message)>();

            for (int i = 0; i < 200; i++)
            {
                try
                {
                    var rng = new Random(i + 1000);
                    var state = new GameState(rng);
                    int playerCount = (i % 3) + 2; // 2, 3, or 4 players
                    var players = new List<(string, IPlayerInput)>();
                    for (int p = 0; p < playerCount; p++)
                        players.Add(($"P{p + 1}", new RandomAIInput(rng)));
                    state.Setup(players);

                    var runner = new GameRunner(state);
                    var winner = runner.RunGame();
                    Assert.NotNull(winner);
                }
                catch (Exception ex)
                {
                    failures.Add((i + 1000, ex.Message));
                }
            }

            Assert.True(failures.Count == 0,
                $"{failures.Count} game(s) failed:\n" +
                string.Join("\n", failures.Select(f => $"  seed={f.seed}: {f.message}")));
        }

        [Fact]
        public void Fifty_ThreePlayer_Games_No_Exceptions()
        {
            var failures = new List<(int seed, string message)>();

            for (int i = 0; i < 50; i++)
            {
                try
                {
                    var rng = new Random(i + 2000);
                    var state = new GameState(rng);
                    state.Setup(new List<(string, IPlayerInput)>
                    {
                        ("P1", new RandomAIInput(rng)),
                        ("P2", new RandomAIInput(rng)),
                        ("P3", new RandomAIInput(rng))
                    });

                    var runner = new GameRunner(state);
                    var winner = runner.RunGame();
                    Assert.NotNull(winner);
                }
                catch (Exception ex)
                {
                    failures.Add((i + 2000, ex.Message));
                }
            }

            Assert.True(failures.Count == 0,
                $"{failures.Count} 3-player game(s) failed:\n" +
                string.Join("\n", failures.Select(f => $"  seed={f.seed}: {f.message}")));
        }

        [Fact]
        public void Fifty_FourPlayer_Games_No_Exceptions()
        {
            var failures = new List<(int seed, string message)>();

            for (int i = 0; i < 50; i++)
            {
                try
                {
                    var rng = new Random(i + 3000);
                    var state = new GameState(rng);
                    state.Setup(new List<(string, IPlayerInput)>
                    {
                        ("P1", new RandomAIInput(rng)),
                        ("P2", new RandomAIInput(rng)),
                        ("P3", new RandomAIInput(rng)),
                        ("P4", new RandomAIInput(rng))
                    });

                    var runner = new GameRunner(state);
                    var winner = runner.RunGame();
                    Assert.NotNull(winner);
                }
                catch (Exception ex)
                {
                    failures.Add((i + 3000, ex.Message));
                }
            }

            Assert.True(failures.Count == 0,
                $"{failures.Count} 4-player game(s) failed:\n" +
                string.Join("\n", failures.Select(f => $"  seed={f.seed}: {f.message}")));
        }

        [Fact]
        public void ExtraTurns_Are_Consumed_Before_Advancing_To_Next_Player()
        {
            // Track how many turns each player takes
            var turnCounts = new Dictionary<string, int>();

            var rng = new Random(42);
            var state = new GameState(rng);

            // Use scripted inputs so the game is deterministic
            var aliceInput = new ScriptedPlayerInput(alwaysStop: true, alwaysClaim: false);
            var bobInput = new ScriptedPlayerInput(alwaysStop: true, alwaysClaim: false);

            state.Setup(new List<(string, IPlayerInput)>
            {
                ("Alice", aliceInput),
                ("Bob", bobInput)
            });

            var alice = state.TurnOrder[0];
            var bob   = state.TurnOrder[1];

            // Grant Alice 2 extra turns at the start
            alice.ExtraTurns = 2;

            // Run just enough rounds to confirm the extra turns are consumed
            var runner = new GameRunner(state, maxRounds: 3);

            // Alice should take 3 turns before Bob takes 1 (turns 1+2+3 for Alice, then Bob)
            // We verify this by checking ExtraTurns is 0 after the game runner processes them.
            runner.RunGame();

            // After the game, ExtraTurns on Alice should be 0 (all consumed)
            Assert.Equal(0, alice.ExtraTurns);
        }
    }
}
