using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Priest: AllLocked, gain 2 tokens when all dice are locked.
    /// </summary>
    public class PriestAbility : Ability
    {
        public PriestAbility()
        {
            TriggerType = TriggerType.AllLocked;
            IsPerTurn = true;
            EntityName = "Priest Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            player.Tokens += 2;
        }
    }
}
