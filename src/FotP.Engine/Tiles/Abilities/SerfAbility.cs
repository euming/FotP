using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Serf: StartOfTurn, add an orange (Serf) die to cup.
    /// </summary>
    public class SerfAbility : Ability
    {
        public SerfAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsPerTurn = true;
            EntityName = "Serf Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var die = new Die(DieType.Serf) { IsTemporary = true };
            player.DicePool.Add(die);
            state.TurnState.Zones.Cup.Add(die);
            state.TurnState.Zones.Temporary.Add(die);
        }
    }
}
