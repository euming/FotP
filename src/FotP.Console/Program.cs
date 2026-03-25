using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            System.Console.WriteLine("=== Favor of the Pharaoh ===\n");
            System.Console.WriteLine("Choose mode:");
            System.Console.WriteLine("  [1] Human vs AI");
            System.Console.WriteLine("  [2] AI vs AI (watch)");
            System.Console.WriteLine("  [3] Run 100 AI games (stress test)");
            System.Console.Write("Choice: ");

            var choice = System.Console.ReadLine()?.Trim() ?? "2";

            switch (choice)
            {
                case "1":
                    RunHumanVsAI();
                    break;
                case "2":
                    RunAIvsAI();
                    break;
                case "3":
                    RunStressTest();
                    break;
                default:
                    RunAIvsAI();
                    break;
            }
        }

        static void RunHumanVsAI()
        {
            var rng = new Random();
            var state = new GameState(rng);
            state.Setup(new List<(string, IPlayerInput)>
            {
                ("Human", new ConsolePlayerInput()),
                ("AI", new RandomAIInput(rng))
            });

            var runner = new GameRunner(state);
            var winner = runner.RunGame();
            System.Console.WriteLine($"\n*** Winner: {winner.Name}! ***");
        }

        static void RunAIvsAI()
        {
            var rng = new Random(42);
            var state = new GameState(rng);
            var ai1 = new VerboseAIInput(new RandomAIInput(rng), "Alice");
            var ai2 = new VerboseAIInput(new RandomAIInput(rng), "Bob");

            state.Setup(new List<(string, IPlayerInput)>
            {
                ("Alice", ai1),
                ("Bob", ai2)
            });

            var runner = new GameRunner(state);
            var winner = runner.RunGame();
            System.Console.WriteLine($"\n*** Winner: {winner.Name}! ***");
        }

        static void RunStressTest()
        {
            int wins1 = 0, wins2 = 0, errors = 0;
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
                    if (winner.Name == "P1") wins1++;
                    else wins2++;
                }
                catch (Exception ex)
                {
                    errors++;
                    System.Console.WriteLine($"Game {i}: ERROR - {ex.Message}");
                }
            }

            System.Console.WriteLine($"\n=== Stress Test Results ===");
            System.Console.WriteLine($"P1 wins: {wins1}");
            System.Console.WriteLine($"P2 wins: {wins2}");
            System.Console.WriteLine($"Errors: {errors}");
        }
    }

    /// <summary>
    /// Wraps an AI input to print decisions to console.
    /// </summary>
    class VerboseAIInput : IPlayerInput
    {
        private readonly IPlayerInput _inner;
        private readonly string _name;

        public VerboseAIInput(IPlayerInput inner, string name)
        {
            _inner = inner;
            _name = name;
        }

        public List<FotP.Engine.Dice.Die> ChooseDiceToLock(IReadOnlyList<FotP.Engine.Dice.Die> activeDice, Player player)
        {
            var result = _inner.ChooseDiceToLock(activeDice, player);
            System.Console.WriteLine($"  {_name} locks: {string.Join(", ", result)}");
            return result;
        }

        public bool ChooseContinueRolling(Player player)
        {
            var result = _inner.ChooseContinueRolling(player);
            System.Console.WriteLine($"  {_name} {(result ? "continues rolling" : "stops")}");
            return result;
        }

        public FotP.Engine.Tiles.Tile? ChooseTileToClaim(IReadOnlyList<FotP.Engine.Tiles.Tile> claimable, Player player)
        {
            var result = _inner.ChooseTileToClaim(claimable, player);
            System.Console.WriteLine($"  {_name} claims: {result?.Name ?? "nothing"}");
            return result;
        }

        public FotP.Engine.Dice.Die? ChooseDie(IReadOnlyList<FotP.Engine.Dice.Die> dice, string prompt, Player player)
            => _inner.ChooseDie(dice, prompt, player);

        public List<FotP.Engine.Dice.Die> ChooseMultipleDice(IReadOnlyList<FotP.Engine.Dice.Die> dice, string prompt, Player player)
            => _inner.ChooseMultipleDice(dice, prompt, player);

        public int ChoosePipValue(FotP.Engine.Dice.Die die, string prompt, Player player)
            => _inner.ChoosePipValue(die, prompt, player);

        public FotP.Engine.Dice.Scarab? ChooseScarab(IReadOnlyList<FotP.Engine.Dice.Scarab> scarabs, Player player)
            => _inner.ChooseScarab(scarabs, player);

        public bool ChooseYesNo(string prompt, Player player) => _inner.ChooseYesNo(prompt, player);
        public bool ChooseUseAbility(FotP.Engine.Tiles.Ability ability, Player player) => _inner.ChooseUseAbility(ability, player);
    }
}
