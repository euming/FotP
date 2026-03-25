using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Soothsayer: AfterRoll (first roll only), adjust 1 active die to any value.
    /// </summary>
    public class SoothsayerAbility : Ability
    {
        public SoothsayerAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            RollNumberFilter = 1;
            EntityName = "Soothsayer Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Soothsayer: Choose a die to adjust", player);
            if (die != null)
            {
                int value = player.Input.ChoosePipValue(die, "Soothsayer: Choose new value", player);
                die.SetValue(value);
            }
        }
    }
}
