using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Grand Vizier: AfterRoll, adjust 1 active die to any value (once per turn).
    /// The Grand Vizier can decree what any die shows.
    /// </summary>
    public class GrandVizierAbility : Ability
    {
        public GrandVizierAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            EntityName = "Grand Vizier Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue).ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Grand Vizier: Choose a die to set to any value", player);
            if (die != null)
            {
                int value = player.Input.ChoosePipValue(die, "Grand Vizier: Choose new value", player);
                die.SetValue(value);
            }
        }
    }
}
