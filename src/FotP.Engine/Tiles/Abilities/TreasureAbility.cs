using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Treasure: Artifact, Acquire, grant 2 additional tile claims this turn
    /// (can claim from two different level slots).
    /// </summary>
    public class TreasureAbility : Ability
    {
        public TreasureAbility()
        {
            TriggerType = TriggerType.Acquire;
            IsArtifact = true;
            EntityName = "Treasure Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            player.AdditionalClaims += 2;
        }
    }
}
