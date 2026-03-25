using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Astrologer (Blue L6): AfterRoll, adjust 1 active die to any value. Once per turn.
    /// </summary>
    public class AstrologerAbility : Ability
    {
        public AstrologerAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            EntityName = "Astrologer Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue).ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Astrologer: Choose a die to adjust", player);
            if (die != null)
            {
                int value = player.Input.ChoosePipValue(die, "Astrologer: Choose new value", player);
                die.SetValue(value);
            }
        }
    }
}
