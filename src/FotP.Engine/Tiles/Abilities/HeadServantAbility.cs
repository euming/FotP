using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Head Servant: AfterRoll, reroll up to 2 active dice (once per roll).
    /// </summary>
    public class HeadServantAbility : Ability
    {
        public HeadServantAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerRoll = true;
            EntityName = "Head Servant Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.ToList();
            if (activeDice.Count == 0) return;

            // Choose up to 2 dice
            var die1 = player.Input.ChooseDie(activeDice, "Head Servant: Choose 1st die to reroll", player);
            if (die1 != null)
            {
                die1.Roll(state.Rng);
                activeDice.Remove(die1);
            }

            if (activeDice.Count > 0)
            {
                var die2 = player.Input.ChooseDie(activeDice, "Head Servant: Choose 2nd die to reroll (or skip)", player);
                if (die2 != null)
                    die2.Roll(state.Rng);
            }
        }
    }
}
