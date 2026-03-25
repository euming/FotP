using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Royal Power: Artifact, Acquire, grant 1 extra turn and 2 additional tile claims.
    /// The ultimate artifact representing supreme authority.
    /// </summary>
    public class RoyalPowerAbility : Ability
    {
        public RoyalPowerAbility()
        {
            TriggerType = TriggerType.Acquire;
            IsArtifact = true;
            EntityName = "Royal Power Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            player.ExtraTurns++;
            player.AdditionalClaims += 2;
        }
    }
}
