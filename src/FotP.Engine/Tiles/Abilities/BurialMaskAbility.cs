using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Burial Mask: Artifact, StartOfTurn, gain 3 tokens.
    /// </summary>
    public class BurialMaskAbility : Ability
    {
        public BurialMaskAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsArtifact = true;
            EntityName = "Burial Mask Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            player.Tokens += 3;
        }
    }
}
