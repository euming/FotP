using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Secret Passage: Artifact, Acquire, grant 1 additional tile claim this turn.
    /// </summary>
    public class SecretPassageAbility : Ability
    {
        public SecretPassageAbility()
        {
            TriggerType = TriggerType.Acquire;
            IsArtifact = true;
            EntityName = "Secret Passage Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            player.AdditionalClaims++;
        }
    }
}
