using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Priestess: AfterRoll, reroll ALL active dice (once per turn).
    /// </summary>
    public class PriestessAbility : Ability
    {
        public PriestessAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            EntityName = "Priestess Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.ToList();
            foreach (var die in activeDice)
                die.Roll(state.Rng);
        }
    }
}
