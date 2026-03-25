using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Entertainer: AfterRoll, reroll 1+ active dice.
    /// </summary>
    public class EntertainerAbility : Ability
    {
        public EntertainerAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerRoll = true;
            EntityName = "Entertainer Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.ToList();
            if (activeDice.Count == 0) return;
            var dice = player.Input.ChooseMultipleDice(activeDice, "Entertainer: Choose dice to reroll", player);
            foreach (var die in dice)
                die.Roll(state.Rng);
        }
    }
}
