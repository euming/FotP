using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Ancestral Guidance: Artifact, AfterRoll, adjust 1 active die to any value.
    /// </summary>
    public class AncestralGuidanceAbility : Ability
    {
        public AncestralGuidanceAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsArtifact = true;
            EntityName = "Ancestral Guidance Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue).ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Ancestral Guidance: Choose a die to adjust", player);
            if (die != null)
            {
                int value = player.Input.ChoosePipValue(die, "Ancestral Guidance: Choose new value", player);
                die.SetValue(value);
            }
        }
    }
}
