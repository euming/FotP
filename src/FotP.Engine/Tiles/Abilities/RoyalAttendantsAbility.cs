using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Royal Attendants: StartOfTurn, add 2 temporary Standard dice to the cup.
    /// </summary>
    public class RoyalAttendantsAbility : Ability
    {
        public RoyalAttendantsAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsPerTurn = true;
            EntityName = "Royal Attendants Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            for (int i = 0; i < 2; i++)
            {
                var die = new Die(DieType.Standard) { IsTemporary = true };
                player.DicePool.Add(die);
                state.TurnState.Zones.Cup.Add(die);
                state.TurnState.Zones.Temporary.Add(die);
            }
        }
    }
}
