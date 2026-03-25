using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Embalmer: AfterRoll, lock 1 active die without it consuming your mandatory lock step.
    /// The die is moved to Locked and contributes to the pyramid but the active dice remain available.
    /// </summary>
    public class EmbalmerAbility : Ability
    {
        public EmbalmerAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            EntityName = "Embalmer Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Embalmer: Choose a die to lock for free", player);
            if (die != null)
                state.TurnState.Zones.LockDie(die);
        }
    }
}
