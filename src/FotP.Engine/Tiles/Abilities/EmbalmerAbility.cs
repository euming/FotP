using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Embalmer (Yellow L6): AfterRoll, bring 1 new standard temporary die into play showing a 6. Once per turn.
    /// </summary>
    public class EmbalmerAbility : Ability
    {
        public EmbalmerAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            EntityName = "Embalmer Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            // Bring a new standard die into play at value 6
            var die = new Die(DieType.Standard) { IsTemporary = true };
            die.SetValue(6);
            player.DicePool.Add(die);
            state.TurnState.Zones.Active.Add(die);
            state.TurnState.Zones.Temporary.Add(die);
        }
    }
}
