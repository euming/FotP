using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Estate Overseer: Two triggers:
    /// - EndOfTurn: set aside 1 active or locked die (store on the tile, remove from pool).
    /// - StartOfTurn: if a die is stored, add it to the cup at its stored value.
    /// </summary>
    public class EstateOverseerStartAbility : Ability
    {
        public EstateOverseerStartAbility()
        {
            TriggerType = TriggerType.StartOfTurn;
            IsPerTurn = true;
            EntityName = "Estate Overseer (Start) Ability";
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

    public class EstateOverseerEndAbility : Ability
    {
        public EstateOverseerEndAbility()
        {
            TriggerType = TriggerType.EndOfTurn;
            IsPerTurn = true;
            EntityName = "Estate Overseer (End) Ability";
        }

        public override bool CanActivate(GameState state, Player player)
        {
            if (!base.CanActivate(state, player)) return false;
            return ParentTile?.StoredDie == null; // Can only store if slot is empty
        }

        public override void Execute(GameState state, Player player)
        {
            // Choose from active or locked dice
            var candidates = state.TurnState.Zones.Active.Concat(state.TurnState.Zones.Locked).ToList();
            if (candidates.Count == 0) return;
            var die = player.Input.ChooseDie(candidates, "Estate Overseer: Choose a die to store for next turn", player);
            if (die == null) return;

            // Remove from all zones and pool — it persists on the tile
            state.TurnState.Zones.Active.Remove(die);
            state.TurnState.Zones.Locked.Remove(die);
            state.TurnState.Zones.Temporary.Remove(die);
            state.TurnState.Zones.Cup.Remove(die);
            state.TurnState.Zones.SetAside.Remove(die);
            player.DicePool.Remove(die);
            die.IsLocked = false;
            die.IsTemporary = false;
            ParentTile!.StoredDie = die;
        }
    }
}
