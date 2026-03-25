using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Tomb Builder (Blue L5): AfterRoll, lock 1 active die at any chosen value. Once per roll.
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
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue).ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Tomb Builder: Choose a die to lock at chosen value", player);
            if (die == null) return;
            int value = player.Input.ChoosePipValue(die, "Tomb Builder: Choose value to lock at", player);
            die.SetValue(value);
            state.TurnState.Zones.LockDie(die);
        }
    }
}
