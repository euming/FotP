using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Servant: AfterRoll, reroll 1 active die.
    /// </summary>
    public class ServantAbility : Ability
    {
        public ServantAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerRoll = true;
            EntityName = "Servant Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Servant: Choose a die to reroll", player);
            if (die != null)
            {
                die.Roll(state.Rng);
            }
        }
    }
}
