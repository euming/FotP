using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Royal Decree: Artifact, StartOfTurn, add 1 temporary Decree die to the cup.
    /// </summary>
    public class RoyalDecreeAbility : Ability
    {
        public RoyalDecreeAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsArtifact = true;
            EntityName = "Royal Decree Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var die = new Die(DieType.Decree) { IsTemporary = true };
            player.DicePool.Add(die);
            state.TurnState.Zones.Cup.Add(die);
            state.TurnState.Zones.Temporary.Add(die);
        }
    }
}
