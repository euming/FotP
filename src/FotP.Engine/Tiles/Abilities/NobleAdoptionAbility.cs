using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Noble Adoption: StartOfTurn, add yellow (Noble) die to cup.
    /// </summary>
    public class NobleAdoptionAbility : Ability
    {
        public NobleAdoptionAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsPerTurn = true;
            EntityName = "Noble Adoption Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var die = new Die(DieType.Noble) { IsTemporary = true };
            player.DicePool.Add(die);
            state.TurnState.Zones.Cup.Add(die);
            state.TurnState.Zones.Temporary.Add(die);
        }
    }
}
