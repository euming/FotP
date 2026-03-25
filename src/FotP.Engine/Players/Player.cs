using System.Linq;
using FotP.Engine.Core;
using FotP.Engine.Dice;
using FotP.Engine.Tiles;

namespace FotP.Engine.Players
{
    public class Player : GameEntity
    {
        public string Name { get; }
        public IPlayerInput Input { get; }

        public SmartList<Tile> OwnedTiles { get; } = new();
        public SmartList<Die> DicePool { get; } = new();
        public SmartList<Scarab> Scarabs { get; } = new();

        /// <summary>Token count for token-based abilities.</summary>
        public int Tokens { get; set; }

        /// <summary>Extra turns granted by abilities (e.g., Omen, Good Omen). Consumed by the game runner.</summary>
        public int ExtraTurns { get; set; }

        /// <summary>Additional tile claims allowed this turn (e.g., Secret Passage, Royal Power). Reset each turn.</summary>
        public int AdditionalClaims { get; set; }

        /// <summary>Modifier to number of standard dice rolled next turn (positive = more, negative = fewer). Reset each turn.</summary>
        public int StandardDiceModifierNextTurn { get; set; }

        public Player(string name, IPlayerInput input)
        {
            Name = name;
            Input = input;
            EntityName = name;
        }

        /// <summary>Sum of pip values of all locked dice that have pip values.</summary>
        public int PyramidScore => DicePool.Where(d => d.IsLocked && d.HasPipValue).Sum(d => d.PipValue);

        public SmartList<Tile> OwnedByColor(TileColor color)
        {
            var result = new SmartList<Tile>();
            foreach (var t in OwnedTiles.Where(t => t.Color == color))
                result.Add(t);
            return result;
        }

        public SmartList<Tile> OwnedByLevel(int level)
        {
            var result = new SmartList<Tile>();
            foreach (var t in OwnedTiles.Where(t => t.Level == level))
                result.Add(t);
            return result;
        }

        public override string ToString() => Name;
    }
}
