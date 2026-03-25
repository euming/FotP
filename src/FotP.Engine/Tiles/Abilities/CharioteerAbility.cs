using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Charioteer (Yellow L5): StartOfTurn, add +1 white (Standard) AND +1 Standard temporary die to the cup.
    /// </summary>
    public class CharioteerAbility : Ability
    {
        public CharioteerAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsPerTurn = true;
            EntityName = "Charioteer Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            // +1 white die (Standard) and +1 Standard die
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
