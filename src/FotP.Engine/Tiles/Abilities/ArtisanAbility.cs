using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Artisan: AfterRoll (1st roll: +1 pip, 2nd roll: +1 pip).
    /// </summary>
    public class ArtisanAbility : Ability
    {
        public ArtisanAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerRoll = true;
            EntityName = "Artisan Ability";
        }

        public override bool CanActivate(GameState state, Player player)
        {
            if (!base.CanActivate(state, player)) return false;
            return state.TurnState.RollCount <= 2; // Only on 1st and 2nd rolls
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            if (activeDice.Count == 0) return;
            var die = player.Input.ChooseDie(activeDice, "Artisan: Choose a die to add +1 pip", player);
            if (die != null) die.TempPipModifier++;
        }
    }
}
