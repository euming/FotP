using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Surveyor (Blue, Level 6): After-roll ability. Replace 1 active die with 2 immediate
    /// dice whose pip values sum to the replaced die's pip value. The 2 new dice are
    /// temporary Immediate dice and will auto-lock before the player's next roll.
    /// </summary>
    public class SurveyorAbility : Ability
    {
        public SurveyorAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            EntityName = "Surveyor Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue).ToList();
            if (activeDice.Count == 0) return;

            var chosen = player.Input.ChooseDie(activeDice, "Surveyor: Choose an active die to split into 2 immediate dice", player);
            if (chosen == null) return;

            int originalValue = chosen.PipValue;
            if (originalValue < 2) return; // Can't split a 1 into two >= 1 dice

            // Player chooses the first die's value (1..originalValue-1); second = remainder
            int firstValue = player.Input.ChoosePipValue(chosen, $"Surveyor: Choose pip value for first die (1–{originalValue - 1}); second die gets the remainder", player);
            firstValue = System.Math.Clamp(firstValue, 1, originalValue - 1);
            int secondValue = originalValue - firstValue;

            // Remove original die from active zone (and pool if it was temporary)
            state.TurnState.Zones.Active.Remove(chosen);
            if (chosen.IsTemporary)
                player.DicePool.Remove(chosen);

            // Create 2 temporary Immediate dice with the split values
            var die1 = new Die(DieType.Immediate) { IsTemporary = true };
            die1.SetValue(firstValue);
            var die2 = new Die(DieType.Immediate) { IsTemporary = true };
            die2.SetValue(secondValue);

            player.DicePool.Add(die1);
            player.DicePool.Add(die2);
            state.TurnState.Zones.Active.Add(die1);
            state.TurnState.Zones.Active.Add(die2);
        }
    }
}
