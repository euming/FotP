using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Conspirator: LockedAny, add +1 pip to 1 locked die (once per turn).
    /// </summary>
    public class ConspiratorAbility : Ability
    {
        public ConspiratorAbility()
        {
            TriggerType = TriggerType.LockedAny;
            IsPerTurn = true;
            EntityName = "Conspirator Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var lockedDice = state.TurnState.Zones.Locked.Where(d => d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            if (lockedDice.Count == 0) return;
            var die = player.Input.ChooseDie(lockedDice, "Conspirator: Choose a locked die to add +1 pip", player);
            if (die != null) die.TempPipModifier++;
        }
    }
}
