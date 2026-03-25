using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Tiles;

namespace FotP.Engine.Players
{
    /// <summary>
    /// AI player that makes random legal choices.
    /// </summary>
    public class RandomAIInput : IPlayerInput
    {
        private readonly Random _rng;

        public RandomAIInput(Random rng)
        {
            _rng = rng;
        }

        public List<Die> ChooseDiceToLock(IReadOnlyList<Die> activeDice, Player player)
        {
            if (activeDice.Count == 0) return new List<Die>();
            // Lock 1 to all dice randomly
            int count = _rng.Next(1, activeDice.Count + 1);
            return activeDice.OrderBy(_ => _rng.Next()).Take(count).ToList();
        }

        public bool ChooseContinueRolling(Player player)
        {
            return _rng.Next(2) == 0; // 50% chance
        }

        public Tile? ChooseTileToClaim(IReadOnlyList<Tile> claimable, Player player)
        {
            if (claimable.Count == 0) return null;
            // Prefer higher level tiles
            var sorted = claimable.OrderByDescending(t => t.Level).ToList();
            return sorted[0]; // Always take the best available
        }

        public Die? ChooseDie(IReadOnlyList<Die> dice, string prompt, Player player)
        {
            if (dice.Count == 0) return null;
            return dice[_rng.Next(dice.Count)];
        }

        public List<Die> ChooseMultipleDice(IReadOnlyList<Die> dice, string prompt, Player player)
        {
            if (dice.Count == 0) return new List<Die>();
            int count = _rng.Next(1, dice.Count + 1);
            return dice.OrderBy(_ => _rng.Next()).Take(count).ToList();
        }

        public int ChoosePipValue(Die die, string prompt, Player player)
        {
            var faces = die.GetFaces();
            var validFaces = faces.Where(f => f > 0).Distinct().ToList();
            if (validFaces.Count == 0) return 1;
            return validFaces[_rng.Next(validFaces.Count)];
        }

        public Scarab? ChooseScarab(IReadOnlyList<Scarab> scarabs, Player player)
        {
            if (scarabs.Count == 0) return null;
            return _rng.Next(2) == 0 ? scarabs[_rng.Next(scarabs.Count)] : null;
        }

        public bool ChooseYesNo(string prompt, Player player) => _rng.Next(2) == 0;

        public bool ChooseUseAbility(Ability ability, Player player) => true; // Always use abilities

        public Player? ChoosePlayer(IReadOnlyList<Player> players, string prompt, Player activePlayer)
        {
            if (players.Count == 0) return null;
            return players[_rng.Next(players.Count)];
        }
    }
}
