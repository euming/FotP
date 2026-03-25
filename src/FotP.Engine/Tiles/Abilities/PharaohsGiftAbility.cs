using FotP.Engine.Players;
using FotP.Engine.State;

namespace FotP.Engine.Tiles.Abilities
{
    /// <summary>
    /// Pharaoh's Gift: Artifact, during the roll-off, after all dice are locked
    /// the player may adjust any number of their locked dice by +1 or -1.
    /// Implemented as AllLocked trigger that adjusts pip modifiers.
    /// </summary>
    public class PharaohsGiftAbility : Ability
    {
        public PharaohsGiftAbility()
        {
            TriggerType = TriggerType.AllLocked;
            IsArtifact = true;
            EntityName = "Pharaoh's Gift Ability";
        }

        public override bool CanActivate(GameState state, Player player)
        {
            if (!base.CanActivate(state, player)) return false;
            return state.Phase == GamePhase.RollOff;
        }

        public override void Execute(GameState state, Player player)
        {
            var lockedDice = state.TurnState.Zones.GetLockedDiceWithPips();
            foreach (var die in lockedDice)
            {
                bool adjust = player.Input.ChooseYesNo($"Pharaoh's Gift: Adjust die showing {die.PipValue}?", player);
                if (adjust)
                {
                    bool up = player.Input.ChooseYesNo($"Pharaoh's Gift: +1 (yes) or -1 (no)?", player);
                    die.TempPipModifier += up ? 1 : -1;
                }
            }
        }
    }
}
