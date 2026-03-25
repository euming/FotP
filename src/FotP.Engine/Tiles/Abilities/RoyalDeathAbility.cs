using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Royal Death: Artifact, Acquire, sets a roll-off bar score that other players
    /// must beat during the final roll-off. The bar is the acquiring player's current pyramid score.
    /// </summary>
    public class RoyalDeathAbility : Ability
    {
        public RoyalDeathAbility()
        {
            TriggerType = TriggerType.Acquire;
            IsArtifact = true;
            EntityName = "Royal Death Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            // Set the roll-off bar to this player's current locked score
            state.RollOffBarScore = player.PyramidScore;
        }
    }
}
