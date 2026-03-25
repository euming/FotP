using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Ankh: Artifact, reroll any number of active dice.
    /// </summary>
    public class AnkhAbility : Ability
    {
        public AnkhAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsArtifact = true;
            EntityName = "Ankh Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.ToList();
            if (activeDice.Count == 0) return;
            var dice = player.Input.ChooseMultipleDice(activeDice, "Ankh: Choose dice to reroll", player);
            foreach (var die in dice)
                die.Roll(state.Rng);
        }
    }
}
