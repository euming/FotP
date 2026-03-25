using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Priestess (Yellow L6): AfterRoll, reroll 1+ chosen active dice AND +1 pip on 1 active die. Once per turn.
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
            if (activeDice.Count == 0) return;

            // Reroll 1 or more chosen active dice
            var diceToReroll = player.Input.ChooseMultipleDice(activeDice, "Priestess: Choose dice to reroll (1+)", player);
            foreach (var die in diceToReroll)
                die.Roll(state.Rng);

            // +1 pip on 1 active die
            var boostable = activeDice.Where(d => d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            if (boostable.Count == 0) return;
            var dieToBoost = player.Input.ChooseDie(boostable, "Priestess: Choose a die to gain +1 pip", player);
            if (dieToBoost != null)
                dieToBoost.TempPipModifier++;
        }
    }
}
