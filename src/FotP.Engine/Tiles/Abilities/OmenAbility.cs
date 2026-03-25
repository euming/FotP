using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Omen: Artifact, AfterRoll (first roll only), grant 1 extra turn after this turn ends.
    /// </summary>
    public class OmenAbility : Ability
    {
        public OmenAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsArtifact = true;
            RollNumberFilter = 1;
            EntityName = "Omen Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            player.ExtraTurns++;
        }
    }
}
