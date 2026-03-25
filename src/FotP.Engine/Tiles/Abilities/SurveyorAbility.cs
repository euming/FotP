using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Surveyor: Acquire (one-time), permanently replace 1 Standard die in the player's pool
    /// with a Noble die.
    /// </summary>
    public class SurveyorAbility : Ability
    {
        public SurveyorAbility()
        {
            TriggerType = TriggerType.Acquire;
            IsArtifact = false; // Not red, but one-time Acquire effect — use IsPerTurn as guard
            IsPerTurn = true;
            EntityName = "Surveyor Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var standard = player.DicePool.Where(d => d.DieType == DieType.Standard && !d.IsTemporary).ToList();
            if (standard.Count == 0) return;

            var toReplace = player.Input.ChooseDie(standard, "Surveyor: Choose a Standard die to replace with Noble", player);
            if (toReplace == null) return;

            player.DicePool.Remove(toReplace);
            var noble = new Die(DieType.Noble);
            player.DicePool.Add(noble);
        }
    }
}
