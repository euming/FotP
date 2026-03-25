using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Indentured Worker: AfterRoll (first roll only), reroll 1 active die.
    /// </summary>
    public class IndenturedWorkerAbility : Ability
    {
        public IndenturedWorkerAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            RollNumberFilter = 1;
            EntityName = "Indentured Worker Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Indentured Worker: Choose a die to reroll", player);
            if (die != null)
                die.Roll(state.Rng);
        }
    }
}
