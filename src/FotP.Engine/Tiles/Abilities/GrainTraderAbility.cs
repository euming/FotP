using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Grain Trader (Blue L6): AfterRoll, +1 pip on 1 active die. Usable after each roll (no per-turn/per-roll limit).
    /// </summary>
    public class GrainTraderAbility : Ability
    {
        public GrainTraderAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            EntityName = "Grain Trader Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Grain Trader: Choose a die to gain +1 pip", player);
            if (die != null) die.TempPipModifier++;
        }
    }
}
