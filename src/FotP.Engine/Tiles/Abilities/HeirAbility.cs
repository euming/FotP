using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Heir: StartOfTurn, add 1 temporary Noble die to the cup.
    /// The heir brings noble lineage to the dice pool.
    /// </summary>
    public class HeirAbility : Ability
    {
        public HeirAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsPerTurn = true;
            EntityName = "Heir Ability";
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
