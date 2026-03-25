using FotP.Engine.Core;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles
{
    /// <summary>
    /// Abstract base for all tile abilities.
    /// </summary>
    public abstract class Ability : GameEntity
    {
        public TriggerType TriggerType { get; protected set; }
        public DieTypeFilter DieTypeFilter { get; protected set; } = DieTypeFilter.Any;
        public bool IsPerTurn { get; protected set; }
        public bool IsPerRoll { get; protected set; }
        public bool IsArtifact { get; protected set; }
        public bool IsUsedThisTurn { get; set; }
        public bool IsUsedThisRoll { get; set; }

        /// <summary>Optional: only triggers on the Nth roll (1-based). 0 = any roll.</summary>
        public int RollNumberFilter { get; protected set; }

        public abstract void Execute(GameState state, Player player);

        public virtual bool CanActivate(GameState state, Player player)
        {
            if (IsPerTurn && IsUsedThisTurn) return false;
            if (IsPerRoll && IsUsedThisRoll) return false;
            if (IsArtifact && (ParentTile?.IsArtifactUsed ?? false)) return false;
            if (RollNumberFilter > 0 && state.TurnState.RollCount != RollNumberFilter) return false;
            return true;
        }

        public void ResetForTurn()
        {
            IsUsedThisTurn = false;
            IsUsedThisRoll = false;
        }

        public void ResetForRoll()
        {
            IsUsedThisRoll = false;
        }

        public Tile? ParentTile { get; set; }
    }
}
