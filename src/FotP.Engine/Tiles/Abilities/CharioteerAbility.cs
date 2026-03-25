using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Charioteer: StartOfTurn, add 1 temporary Standard die to the cup.
    /// A faster, more powerful worker than the basic Worker tile.
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
            var die = new Die(DieType.Standard) { IsTemporary = true };
            player.DicePool.Add(die);
            state.TurnState.Zones.Cup.Add(die);
            state.TurnState.Zones.Temporary.Add(die);
        }
    }
}
