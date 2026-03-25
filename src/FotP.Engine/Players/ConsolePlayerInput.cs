using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Tiles;

namespace FotP.Engine.Players
{
    /// <summary>
    /// Console-based player input for debug play.
    /// </summary>
    public class ConsolePlayerInput : IPlayerInput
    {
        public List<Die> ChooseDiceToLock(IReadOnlyList<Die> activeDice, Player player)
        {
            Console.WriteLine($"\n{player.Name}'s active dice:");
            for (int i = 0; i < activeDice.Count; i++)
                Console.WriteLine($"  [{i}] {activeDice[i]}");

            Console.Write("Choose dice to lock (comma-separated indices, e.g. 0,2): ");
            var input = Console.ReadLine() ?? "0";
            var indices = input.Split(',', StringSplitOptions.RemoveEmptyEntries)
                .Select(s => int.TryParse(s.Trim(), out int v) ? v : -1)
                .Where(i => i >= 0 && i < activeDice.Count)
                .Distinct()
                .ToList();

            if (indices.Count == 0) indices.Add(0);
            return indices.Select(i => activeDice[i]).ToList();
        }

        public bool ChooseContinueRolling(Player player)
        {
            Console.Write($"{player.Name}: Continue rolling? (y/n): ");
            var input = Console.ReadLine() ?? "n";
            return input.Trim().ToLower().StartsWith("y");
        }

        public Tile? ChooseTileToClaim(IReadOnlyList<Tile> claimable, Player player)
        {
            if (claimable.Count == 0)
            {
                Console.WriteLine("No tiles available to claim.");
                return null;
            }

            Console.WriteLine($"\n{player.Name}'s claimable tiles:");
            Console.WriteLine("  [0] Skip (don't claim)");
            for (int i = 0; i < claimable.Count; i++)
                Console.WriteLine($"  [{i + 1}] {claimable[i]}");

            Console.Write("Choose tile: ");
            var input = Console.ReadLine() ?? "0";
            if (int.TryParse(input.Trim(), out int idx) && idx > 0 && idx <= claimable.Count)
                return claimable[idx - 1];
            return null;
        }

        public Die? ChooseDie(IReadOnlyList<Die> dice, string prompt, Player player)
        {
            Console.WriteLine($"\n{prompt}:");
            for (int i = 0; i < dice.Count; i++)
                Console.WriteLine($"  [{i}] {dice[i]}");
            Console.Write("Choose (or Enter to skip): ");
            var input = Console.ReadLine() ?? "";
            if (int.TryParse(input.Trim(), out int idx) && idx >= 0 && idx < dice.Count)
                return dice[idx];
            return null;
        }

        public List<Die> ChooseMultipleDice(IReadOnlyList<Die> dice, string prompt, Player player)
        {
            Console.WriteLine($"\n{prompt}:");
            for (int i = 0; i < dice.Count; i++)
                Console.WriteLine($"  [{i}] {dice[i]}");
            Console.Write("Choose (comma-separated, or Enter to skip): ");
            var input = Console.ReadLine() ?? "";
            return input.Split(',', StringSplitOptions.RemoveEmptyEntries)
                .Select(s => int.TryParse(s.Trim(), out int v) ? v : -1)
                .Where(i => i >= 0 && i < dice.Count)
                .Distinct()
                .Select(i => dice[i])
                .ToList();
        }

        public int ChoosePipValue(Die die, string prompt, Player player)
        {
            Console.Write($"{prompt} (1-6): ");
            var input = Console.ReadLine() ?? "1";
            if (int.TryParse(input.Trim(), out int val) && val >= 1 && val <= 6)
                return val;
            return 1;
        }

        public Scarab? ChooseScarab(IReadOnlyList<Scarab> scarabs, Player player)
        {
            if (scarabs.Count == 0) return null;
            Console.WriteLine($"\n{player.Name}'s scarabs:");
            Console.WriteLine("  [0] Skip");
            for (int i = 0; i < scarabs.Count; i++)
                Console.WriteLine($"  [{i + 1}] {scarabs[i]}");
            Console.Write("Choose scarab: ");
            var input = Console.ReadLine() ?? "0";
            if (int.TryParse(input.Trim(), out int idx) && idx > 0 && idx <= scarabs.Count)
                return scarabs[idx - 1];
            return null;
        }

        public bool ChooseYesNo(string prompt, Player player)
        {
            Console.Write($"{prompt} (y/n): ");
            var input = Console.ReadLine() ?? "n";
            return input.Trim().ToLower().StartsWith("y");
        }

        public bool ChooseUseAbility(Ability ability, Player player)
        {
            Console.Write($"Use {ability.EntityName}? (y/n): ");
            var input = Console.ReadLine() ?? "y";
            return input.Trim().ToLower().StartsWith("y");
        }

        public Player? ChoosePlayer(IReadOnlyList<Player> players, string prompt, Player activePlayer)
        {
            if (players.Count == 0) return null;
            Console.WriteLine($"\n{prompt}:");
            for (int i = 0; i < players.Count; i++)
                Console.WriteLine($"  [{i}] {players[i].Name} (tokens: {players[i].Tokens})");
            Console.Write("Choose player (or Enter to skip): ");
            var input = Console.ReadLine() ?? "";
            if (int.TryParse(input.Trim(), out int idx) && idx >= 0 && idx < players.Count)
                return players[idx];
            return null;
        }
    }
}
