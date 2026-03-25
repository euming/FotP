using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Farmer: AfterRoll (first roll only), add +1 pip to 1 active die.
    /// </summary>
    public class FarmerAbility : Ability
    {
        public FarmerAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            RollNumberFilter = 1;
            EntityName = "Farmer Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Farmer: Choose a die to add +1 pip", player);
            if (die != null) die.TempPipModifier++;
        }
    }
}
