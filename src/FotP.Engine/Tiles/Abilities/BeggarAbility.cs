using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Beggar: StartOfTurn, gain 1 token.
    /// </summary>
    public class BeggarAbility : Ability
    {
        public BeggarAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsPerTurn = true;
            EntityName = "Beggar Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            player.Tokens++;
        }
    }
}
