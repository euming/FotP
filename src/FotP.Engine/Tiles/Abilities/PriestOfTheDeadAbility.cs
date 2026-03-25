using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Priest of the Dead: AllLocked, adjust 1 locked die to any valid pip value.
    /// </summary>
    public class PriestOfTheDeadAbility : Ability
    {
        public PriestOfTheDeadAbility()
        {
            TriggerType = TriggerType.AllLocked;
            IsPerTurn = true;
            EntityName = "Priest of the Dead Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var lockedDice = state.TurnState.Zones.Locked.Where(d => d.HasPipValue).ToList();
            if (lockedDice.Count == 0) return;
            var die = player.Input.ChooseDie(lockedDice, "Priest of the Dead: Choose a locked die to adjust", player);
            if (die != null)
            {
                int value = player.Input.ChoosePipValue(die, "Priest of the Dead: Choose new value", player);
                die.SetValue(value);
            }
        }
    }
}
