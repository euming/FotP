using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Soldier: AfterRoll, add +1 pip to 1 active die (usable on any roll, once per roll).
    /// Enhanced guard — same mechanical effect but from a blue/combat tile.
    /// </summary>
    public class SoldierAbility : Ability
    {
        public SoldierAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerRoll = true;
            EntityName = "Soldier Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Soldier: Choose a die to add +1 pip", player);
            if (die != null) die.TempPipModifier++;
        }
    }
}
