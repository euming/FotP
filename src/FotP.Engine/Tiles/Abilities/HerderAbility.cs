using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Herder: LockedAny, if any locked pair exists, add +1 temporary Standard die to the cup (once per turn).
    /// The Herder tile has no claim criteria — it can be claimed with any locked dice.
    /// </summary>
    public class HerderAbility : Ability
    {
        public HerderAbility()
        {
            TriggerType = TriggerType.LockedAny;
            IsPerTurn = true;
            EntityName = "Herder Ability";
        }

        public override bool CanActivate(GameState state, Player player)
        {
            if (!base.CanActivate(state, player)) return false;
            // Only fires if there is at least one pair among locked dice
            var locked = state.TurnState.Zones.Locked;
            return locked
                .Where(d => d.HasPipValue)
                .GroupBy(d => d.PipValue)
                .Any(g => g.Count() >= 2);
        }

        public override void Execute(GameState state, Player player)
        {
            var die = new Die(DieType.Standard) { IsTemporary = true };
            player.DicePool.Add(die);
            state.TurnState.Zones.Cup.Add(die);
            state.TurnState.Zones.Temporary.Add(die);
        }
    }
}
