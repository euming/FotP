using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Builder: AfterRoll (any roll), add +1 pip to up to 2 different active dice.
    /// </summary>
    public class BuilderAbility : Ability
    {
        public BuilderAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerRoll = true;
            EntityName = "Builder Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            if (activeDice.Count == 0) return;

            // First die
            var die1 = player.Input.ChooseDie(activeDice, "Builder: Choose 1st die to add +1 pip", player);
            if (die1 != null)
            {
                die1.TempPipModifier++;
                activeDice.Remove(die1);
            }

            // Second die (optional)
            if (activeDice.Count > 0)
            {
                var die2 = player.Input.ChooseDie(activeDice, "Builder: Choose 2nd die to add +1 pip (or skip)", player);
                if (die2 != null)
                    die2.TempPipModifier++;
            }
        }
    }
}
