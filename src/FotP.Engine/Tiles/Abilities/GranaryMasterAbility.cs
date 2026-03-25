using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Granary Master: Two triggers (same stored-die mechanic as Estate Overseer but level 7).
    /// - EndOfTurn: store 1 die on the tile.
    /// - StartOfTurn: if stored, add it to the cup at its stored value.
    /// </summary>
    public class GranaryMasterStartAbility : Ability
    {
        public GranaryMasterStartAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsPerTurn = true;
            EntityName = "Granary Master (Start) Ability";
        }

        public override bool CanActivate(GameState state, Player player)
        {
            if (!base.CanActivate(state, player)) return false;
            return ParentTile?.StoredDie != null;
        }

        public override void Execute(GameState state, Player player)
        {
            var stored = ParentTile!.StoredDie!;
            ParentTile.StoredDie = null;
            stored.IsTemporary = true;
            player.DicePool.Add(stored);
            state.TurnState.Zones.Cup.Add(stored);
            state.TurnState.Zones.Temporary.Add(stored);
        }
    }

    public class GranaryMasterEndAbility : Ability
    {
        public GranaryMasterEndAbility()
        {
            TriggerType = TriggerType.EndOfTurn;
            IsPerTurn = true;
            EntityName = "Granary Master (End) Ability";
        }

        public override bool CanActivate(GameState state, Player player)
        {
            if (!base.CanActivate(state, player)) return false;
            return ParentTile?.StoredDie == null;
        }

        public override void Execute(GameState state, Player player)
        {
            var candidates = state.TurnState.Zones.Active.Concat(state.TurnState.Zones.Locked).ToList();
            if (candidates.Count == 0) return;
            var die = player.Input.ChooseDie(candidates, "Granary Master: Choose a die to store for next turn", player);
            if (die == null) return;

            state.TurnState.Zones.Active.Remove(die);
            state.TurnState.Zones.Locked.Remove(die);
            player.DicePool.Remove(die);
            die.IsLocked = false;
            die.IsTemporary = false;
            ParentTile!.StoredDie = die;
        }
    }
}
