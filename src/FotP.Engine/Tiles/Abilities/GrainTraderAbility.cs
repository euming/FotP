using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Grain Trader: StartOfTurn, gain 1 token per 2 locked dice from previous turn
    /// (approximated as +2 tokens per turn as a simplified implementation).
    /// </summary>
    public class GrainTraderAbility : Ability
    {
        public GrainTraderAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsPerTurn = true;
            EntityName = "Grain Trader Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            player.Tokens += 2;
        }
    }
}
