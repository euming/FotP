using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Palace Servants: StartOfTurn, gain 2 tokens.
    /// </summary>
    public class PalaceServantsAbility : Ability
    {
        public PalaceServantsAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsPerTurn = true;
            EntityName = "Palace Servants Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            player.Tokens += 2;
        }
    }
}
