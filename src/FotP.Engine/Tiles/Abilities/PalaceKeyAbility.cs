using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Palace Key: Artifact, StartOfTurn, add +2 standard dice to cup.
    /// </summary>
    public class PalaceKeyAbility : Ability
    {
        public PalaceKeyAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsArtifact = true;
            EntityName = "Palace Key Ability";
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
