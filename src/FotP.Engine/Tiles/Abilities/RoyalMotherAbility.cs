using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Royal Mother (Blue, Level 7): Replace any number of active Immediate and/or
    /// Serf dice with an equal number of tokens AND one standard die to roll for the
    /// rest of the turn.
    /// </summary>
    public class RoyalMotherAbility : Ability
    {
        public RoyalMotherAbility()
        {
            TriggerType = TriggerType.AfterRoll;
            IsPerTurn = true;
            EntityName = "Royal Mother Ability";
        }

        public override void Execute(GameState state, Player player)
        {
            var eligible = state.TurnState.Zones.Active
                .Where(d => d.DieType == DieType.Immediate || d.DieType == DieType.Serf)
                .ToList();

            if (eligible.Count == 0) return;

            var chosen = player.Input.ChooseMultipleDice(eligible,
                "Royal Mother: Choose Immediate/Serf dice to replace with tokens (+ 1 Standard die)", player);

            if (chosen == null || chosen.Count == 0) return;

            foreach (var die in chosen)
            {
                state.TurnState.Zones.Active.Remove(die);
                player.DicePool.Remove(die);
                player.Tokens++;
            }

            var standard = new Die(DieType.Standard) { IsTemporary = true };
            player.DicePool.Add(standard);
            state.TurnState.Zones.Cup.Add(standard);
            state.TurnState.Zones.Temporary.Add(standard);
        }
    }
}
