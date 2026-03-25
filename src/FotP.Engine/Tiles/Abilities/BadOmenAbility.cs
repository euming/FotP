using System.Linq;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Bad Omen: Artifact, EndOfTurn, force one other player to lose 1 token next turn
    /// (represented by decrementing their tokens immediately).
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
            var others = state.TurnOrder.Where(p => p != player && p.Tokens > 0).ToList();
            if (others.Count == 0) return;

            // The active player chooses which opponent to afflict
            var target = player.Input.ChooseDie(null!, "Bad Omen: Choose target player", player);
            // Use token loss directly on an other player — pick the first other player
            // (full player-choice UI would need ChoosePlayer; use first other as fallback)
            var victim = others[0];
            victim.Tokens = System.Math.Max(0, victim.Tokens - 1);
        }
    }
}
