using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Tomb Builder: AfterRoll, add +1 pip to 1 active die. Usable on any roll (once per roll).
    /// </summary>
    public class TombBuilderAbility : Ability
    {
        public TombBuilderAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerRoll = true;
            EntityName = "Tomb Builder Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Tomb Builder: Choose a die to add +1 pip", player);
            if (die != null) die.TempPipModifier++;
        }
    }
}
