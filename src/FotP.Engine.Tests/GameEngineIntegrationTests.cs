using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FotP.Engine.Players;
using FotP.Engine.State;
using Xunit;

namespace FotP.Engine.Tests
{
    /// <summary>
    /// Integration tests for GameEngine — the same code path used by GameController
    /// in the Unity scene. Validates that full games play from Setup through GameOver
    /// without exceptions, covering 2/3/4-player configurations.
    ///
    /// These tests are the closest equivalent to "play a full game in Unity" that can
    /// run headlessly: they exercise GameEngine.RunGame() with RandomAIInput, exactly
    /// as GameController does on its background Task.Run thread.
    /// </summary>
    public class GameEngineIntegrationTests
    {
        private static GameEngine BuildEngine(int playerCount, int seed)
        {
            var rng = new Random(seed);
            var state = new GameState(rng);
            var configs = new List<(string, IPlayerInput)>();
            for (int i = 0; i < playerCount; i++)
                configs.Add(($"P{i + 1}", new RandomAIInput(rng)));
            state.Setup(configs);
            // GameController sets Phase = Playing after Setup; Setup already does this,
            // so this mirrors the exact sequence in GameController.StartGame().
            state.Phase = GamePhase.Playing;
            return new GameEngine(state);
        }

        // -----------------------------------------------------------------------
        // Single-game smoke tests
        // -----------------------------------------------------------------------

        [Fact]
        public void GameEngine_TwoPlayer_CompletesAndReturnsWinner()
        {
            var engine = BuildEngine(2, 42);
            var winner = engine.RunGame();

            Assert.NotNull(winner);
            Assert.Equal(GamePhase.GameOver, engine.State.Phase);
        }

        [Fact]
        public void GameEngine_ThreePlayer_CompletesAndReturnsWinner()
        {
            var engine = BuildEngine(3, 123);
            var winner = engine.RunGame();

            Assert.NotNull(winner);
            Assert.Equal(GamePhase.GameOver, engine.State.Phase);
        }

        [Fact]
        public void GameEngine_FourPlayer_CompletesAndReturnsWinner()
        {
            var engine = BuildEngine(4, 999);
            var winner = engine.RunGame();

            Assert.NotNull(winner);
            Assert.Equal(GamePhase.GameOver, engine.State.Phase);
        }

        [Fact]
        public void GameEngine_Winner_IsOneOfTheRegisteredPlayers()
        {
            var engine = BuildEngine(3, 7);
            var winner = engine.RunGame();

            Assert.Contains(winner, engine.State.TurnOrder);
        }

        // -----------------------------------------------------------------------
        // Bulk reliability tests — mirrors GameController's Task.Run scenario
        // -----------------------------------------------------------------------

        [Fact]
        public void GameEngine_Hundred_TwoPlayer_Games_NoExceptions()
        {
            var failures = new List<(int seed, string msg)>();

            for (int i = 0; i < 100; i++)
            {
                try
                {
                    var engine = BuildEngine(2, i);
                    var winner = engine.RunGame();
                    Assert.NotNull(winner);
                    Assert.Equal(GamePhase.GameOver, engine.State.Phase);
                }
                catch (Exception ex)
                {
                    failures.Add((i, ex.Message));
                }
            }

            Assert.True(failures.Count == 0,
                $"{failures.Count} game(s) failed:\n" +
                string.Join("\n", failures.Select(f => $"  seed={f.seed}: {f.msg}")));
        }

        [Fact]
        public void GameEngine_Fifty_MixedPlayer_Games_NoExceptions()
        {
            var failures = new List<(int seed, string msg)>();

            for (int i = 0; i < 50; i++)
            {
                try
                {
                    int playerCount = (i % 3) + 2; // cycles 2, 3, 4
                    var engine = BuildEngine(playerCount, i + 5000);
                    var winner = engine.RunGame();
                    Assert.NotNull(winner);
                    Assert.Equal(GamePhase.GameOver, engine.State.Phase);
                }
                catch (Exception ex)
                {
                    failures.Add((i, ex.Message));
                }
            }

            Assert.True(failures.Count == 0,
                $"{failures.Count} game(s) failed:\n" +
                string.Join("\n", failures.Select(f => $"  seed={f.seed}: {f.msg}")));
        }

        // -----------------------------------------------------------------------
        // Background-thread scenario — mirrors Unity GameController.StartGame()
        // -----------------------------------------------------------------------

        [Fact]
        public void GameEngine_RunsToCompletion_OnBackgroundThread()
        {
            // Replicate what GameController does: run engine.RunGame() on Task.Run
            Player winner = null;
            Exception thrown = null;

            var task = Task.Run(() =>
            {
                try
                {
                    var engine = BuildEngine(2, 77);
                    winner = engine.RunGame();
                }
                catch (Exception ex)
                {
                    thrown = ex;
                }
            });

            task.Wait(TimeSpan.FromSeconds(30));
            Assert.True(task.IsCompleted, "Engine did not complete within 30 seconds on background thread");
            Assert.Null(thrown);
            Assert.NotNull(winner);
        }

        // -----------------------------------------------------------------------
        // Roll-off integration via GameEngine
        // -----------------------------------------------------------------------

        [Fact]
        public void GameEngine_RollOff_ReachesGameOver()
        {
            // Run many games; at least one should reach the RollOff path.
            // Verify that when it does, the engine still returns a valid winner.
            bool rollOffHit = false;

            for (int i = 0; i < 30; i++)
            {
                var rng = new Random(i + 9000);
                var state = new GameState(rng);
                state.Setup(new List<(string, IPlayerInput)>
                {
                    ("Alice", new RandomAIInput(rng)),
                    ("Bob",   new RandomAIInput(rng)),
                    ("Carol", new RandomAIInput(rng))
                });
                state.Phase = GamePhase.Playing;

                var engine = new GameEngine(state);
                var winner = engine.RunGame();

                Assert.NotNull(winner);
                Assert.Equal(GamePhase.GameOver, state.Phase);

                if (state.QueenClaimant != null)
                    rollOffHit = true;
            }

            // At least some games should have triggered the roll-off path
            Assert.True(rollOffHit, "No game triggered a roll-off in 30 attempts with 3 players");
        }
    }
}
