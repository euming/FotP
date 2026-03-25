using FotP.Engine.Core;
using FotP.Engine.Criteria;
using FotP.Engine.Dice;

namespace FotP.Engine.Tiles
{
    public class Tile : GameEntity
    {
        public string Name { get; }
        public int Level { get; }
        public TileColor Color { get; }
        public bool IsArtifact => Color == TileColor.Red;
        public bool IsArtifactUsed { get; set; }

        public SmartList<Ability> Abilities { get; } = new();

        /// <summary>The criteria a player must meet with locked dice to claim this tile.</summary>
        public Criterion? ClaimCriteria { get; set; }

        /// <summary>How many copies remain in the market stack.</summary>
        public int StackCount { get; set; }

        /// <summary>A die stored between turns by abilities like Estate Overseer or Granary Master.</summary>
        public Die? StoredDie { get; set; }

        public Tile(string name, int level, TileColor color)
        {
            Name = name;
            Level = level;
            Color = color;
            EntityName = name;
        }

        public void AddAbility(Ability ability)
        {
            ability.ParentTile = this;
            ability.SetParent(this);
            Abilities.Add(ability);
        }

        public override string ToString() => $"{Name} (L{Level} {Color})";
    }
}
