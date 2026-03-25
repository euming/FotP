using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Master Artisan: AfterRoll, add +1 pip to any number of active dice (once per roll).
    /// </summary>
    public class MasterArtisanAbility : Ability
    {
        public MasterArtisanAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerRoll = true;
            EntityName = "Master Artisan Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var activeDice = state.TurnState.Zones.Active.Where(d => d.HasPipValue && d.PipValue < d.MaxValue).ToList();
            if (activeDice.Count == 0) return;
            var chosen = player.Input.ChooseMultipleDice(activeDice, "Master Artisan: Choose dice to add +1 pip each", player);
            foreach (var die in chosen)
                die.TempPipModifier++;
        }
    }
}
