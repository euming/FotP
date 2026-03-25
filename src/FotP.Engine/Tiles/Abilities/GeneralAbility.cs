using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// General: AfterRoll, add +1 pip to ALL active dice (once per roll).
    /// </summary>
    public class GeneralAbility : Ability
    {
        public GeneralAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerRoll = true;
            EntityName = "General Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            foreach (var die in activeDice)
                die.TempPipModifier++;
        }
    }
}
