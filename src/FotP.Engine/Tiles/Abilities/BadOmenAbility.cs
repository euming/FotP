using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Bad Omen (Red L5): Artifact, EndOfTurn. Each other player rolls 2 fewer standard dice next turn.
    /// You roll +1 standard die next turn.
    /// </summary>
    public class BadOmenAbility : Ability
    {
        public BadOmenAbility()
        {
            TriggerType = TriggerType.EndOfTurn;
            IsArtifact = true;
            EntityName = "Bad Omen Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            // Each other player rolls 2 fewer standard dice next turn
            foreach (var other in state.TurnOrder.Where(p => p != player))
                other.StandardDiceModifierNextTurn -= 2;

            // You roll +1 standard die next turn
            player.StandardDiceModifierNextTurn += 1;
        }
    }
}
