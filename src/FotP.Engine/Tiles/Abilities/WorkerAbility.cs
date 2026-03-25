using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Worker: StartOfTurn, add 1 temporary Standard die to the cup.
    /// </summary>
    public class WorkerAbility : Ability
    {
        public WorkerAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsPerTurn = true;
            EntityName = "Worker Ability";
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
