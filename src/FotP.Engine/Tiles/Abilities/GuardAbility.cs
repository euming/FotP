using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Guard: AfterRoll (any roll), add +1 pip to 1 active die.
    /// </summary>
    public class GuardAbility : Ability
    {
        public GuardAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerRoll = true;
            EntityName = "Guard Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Guard: Choose a die to add +1 pip", player);
            if (die != null) die.TempPipModifier++;
        }
    }
}
