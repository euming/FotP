using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Priest (Blue L5): AfterRoll, reroll 1 active die AND +1 pip on another active die. Once per turn.
    /// </summary>
    public class PriestAbility : Ability
    {
        public PriestAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            EntityName = "Priest Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.ToList();
            if (activeDice.Count == 0) return;

            // Reroll 1 active die
            var dieToReroll = player.Input.ChooseDie(activeDice, "Priest: Choose a die to reroll", player);
            if (dieToReroll != null)
                dieToReroll.Roll(state.Rng);

            // +1 pip on another active die (different die)
            var remaining = activeDice.Where(d => d != dieToReroll && d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            if (remaining.Count == 0) return;
            var dieToBoost = player.Input.ChooseDie(remaining, "Priest: Choose a die to gain +1 pip", player);
            if (dieToBoost != null)
                dieToBoost.TempPipModifier++;
        }
    }
}
