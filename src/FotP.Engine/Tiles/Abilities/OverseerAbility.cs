using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Overseer: AfterRoll, add +1 pip to up to 2 active dice (once per turn).
    /// </summary>
    public class OverseerAbility : Ability
    {
        public OverseerAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            EntityName = "Overseer Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            if (activeDice.Count == 0) return;

            var die1 = player.Input.ChooseDie(activeDice, "Overseer: Choose 1st die to add +1 pip", player);
            if (die1 != null)
            {
                die1.TempPipModifier++;
                activeDice.Remove(die1);
            }

            if (activeDice.Count > 0)
            {
                var die2 = player.Input.ChooseDie(activeDice, "Overseer: Choose 2nd die to add +1 pip (or skip)", player);
                if (die2 != null)
                    die2.TempPipModifier++;
            }
        }
    }
}
