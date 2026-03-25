using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Ship Captain: StartOfTurn, add 1 temporary Voyage die to the cup.
    /// </summary>
    public class ShipCaptainAbility : Ability
    {
        public ShipCaptainAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsPerTurn = true;
            EntityName = "Ship Captain Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var die = new Die(DieType.Voyage) { IsTemporary = true };
            player.DicePool.Add(die);
            state.TurnState.Zones.Cup.Add(die);
            state.TurnState.Zones.Temporary.Add(die);
        }
    }
}
