using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Spirit of the Dead: Artifact, AllLocked, adjust 1 locked die to any value.
    /// Represents calling on ancestors to change fate at the last moment.
    /// </summary>
    public class SpiritOfTheDeadAbility : Ability
    {
        public SpiritOfTheDeadAbility()
        {
            TriggerType = TriggerType.AllLocked;
            IsArtifact = true;
            EntityName = "Spirit of the Dead Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var lockedDice = state.TurnState.Zones.Locked.Where(d => d.HasPipValue).ToList();
            if (lockedDice.Count == 0) return;
            var die = player.Input.ChooseDie(lockedDice, "Spirit of the Dead: Choose a locked die to adjust", player);
            if (die != null)
            {
                int value = player.Input.ChoosePipValue(die, "Spirit of the Dead: Choose new value", player);
                die.SetValue(value);
            }
        }
    }
}
