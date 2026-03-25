using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.Tiles;

namespace FotP.Engine.Market
{
    /// <summary>
    /// Manages the tile market (bar slots with tile stacks).
    /// </summary>
    public class MarketManager
    {
        public List<TileStack> Stacks { get; } = new();

        /// <summary>
        /// Gets tiles the player can claim based on their locked dice.
        /// Rules: can only claim from levels where you don't already have that color,
        /// and criteria must be met.
        /// </summary>
        public List<TileStack> GetClaimableStacks(Player player, IReadOnlyList<Die> lockedDice)
        {
            var result = new List<TileStack>();
            foreach (var stack in Stacks)
            {
                if (stack.IsEmpty) continue;
                var tile = stack.Prototype;

                // Check if player already has a tile of this color at this level
                bool alreadyHasColorAtLevel = player.OwnedTiles
                    .Any(t => t.Color == tile.Color && t.Level == tile.Level);
                if (alreadyHasColorAtLevel) continue;

                // Check criteria
                if (tile.ClaimCriteria != null && !tile.ClaimCriteria.Evaluate(lockedDice))
                    continue;

                result.Add(stack);
            }
            return result;
        }

        /// <summary>
        /// Claim a tile: decrement stack, create a copy for the player.
        /// </summary>
        public Tile ClaimTile(Player player, Tile prototype)
        {
            var stack = Stacks.FirstOrDefault(s => s.Prototype == prototype);
            if (stack == null || stack.IsEmpty)
                throw new InvalidOperationException($"No more {prototype.Name} tiles available.");

            stack.Remaining--;

            // Create a new tile instance for the player with same abilities
            var claimed = TileFactory.CreateTile(prototype.Name, prototype.Level, prototype.Color);
            player.OwnedTiles.Add(claimed);

            // Check if this was the last tile in a level (triggers Queen check)
            return claimed;
        }

        /// <summary>Check if any level's tiles are completely gone (triggers end game).</summary>
        public bool IsAnyLevelDepleted()
        {
            var levelGroups = Stacks.GroupBy(s => s.Prototype.Level);
            foreach (var group in levelGroups)
            {
                if (group.All(s => s.IsEmpty))
                    return true;
            }
            return false;
        }
    }
}
