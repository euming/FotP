using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Head Servant (Blue L5): AfterRoll, reroll ANY NUMBER of active dice. Once per roll.
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

            var diceToReroll = player.Input.ChooseMultipleDice(activeDice, "Head Servant: Choose any dice to reroll", player);
            foreach (var die in diceToReroll)
                die.Roll(state.Rng);
        }
    }
}
