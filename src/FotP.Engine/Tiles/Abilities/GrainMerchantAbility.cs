using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Grain Merchant: AfterRoll, gain 1 token (once per roll — tokens accumulate as you keep rolling).
    /// </summary>
    public class GrainMerchantAbility : Ability
    {
        public GrainMerchantAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerRoll = true;
            EntityName = "Grain Merchant Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            player.Tokens++;
        }
    }
}
