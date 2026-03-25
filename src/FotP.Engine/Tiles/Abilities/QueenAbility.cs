using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Queen: Acquire, set the claiming player as QueenClaimant and enter roll-off phase.
    /// This triggers at the end of the claiming player's turn.
    /// </summary>
    public class QueenAbility : Ability
    {
        public QueenAbility()
        {
            TriggerType = TriggerType.Acquire;
            EntityName = "Queen Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            state.QueenClaimant = player;
            state.PharaohHolder = player;
            state.PharaohScore = player.PyramidScore;
            // Phase transitions to RollOff after the current round completes.
            // The game runner checks for QueenClaimant != null to start the roll-off.
        }
    }
}
