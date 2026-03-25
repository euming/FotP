using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Queen's Favor: Artifact, Acquire, grant 1 extra turn and 1 additional tile claim.
    /// </summary>
    public class QueensFavorAbility : Ability
    {
        public QueensFavorAbility()
        {
            TriggerType = TriggerType.Acquire;
            IsArtifact = true;
            EntityName = "Queen's Favor Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            player.ExtraTurns++;
            player.AdditionalClaims++;
        }
    }
}
