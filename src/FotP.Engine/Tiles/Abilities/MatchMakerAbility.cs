using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Match Maker: AfterRoll (first roll only), swap the values of 2 active dice.
    /// </summary>
    public class MatchMakerAbility : Ability
    {
        public MatchMakerAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            RollNumberFilter = 1;
            EntityName = "Match Maker Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue).ToList();
            if (activeDice.Count < 2) return;

            var die1 = player.Input.ChooseDie(activeDice, "Match Maker: Choose first die to swap", player);
            if (die1 == null) return;
            activeDice.Remove(die1);

            var die2 = player.Input.ChooseDie(activeDice, "Match Maker: Choose second die to swap", player);
            if (die2 == null) return;

            // Swap values via temp modifiers so faces stay valid
            int v1 = die1.PipValue;
            int v2 = die2.PipValue;
            die1.TempPipModifier += v2 - v1;
            die2.TempPipModifier += v1 - v2;
        }
    }
}
