using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Core;
using FotP.Engine.Dice;

namespace FotP.Engine.State
{
    /// <summary>
    /// Manages dice zones: Cup (unrolled), Active (just rolled), Locked, Set (aside), Temporary.
    /// </summary>
    public class DiceZoneManager
    {
        public SmartList<Die> Cup { get; } = new();
        public SmartList<Die> Active { get; } = new();
        public SmartList<Die> Locked { get; } = new();
        public SmartList<Die> SetAside { get; } = new();
        public SmartList<Die> Temporary { get; } = new();

        /// <summary>Roll all dice currently in the Cup, moving them to Active.</summary>
        public void RollAllInCup(Random rng)
        {
            var dice = Cup.ToList();
            foreach (var die in dice)
            {
                die.Roll(rng);
                Cup.Remove(die);
                Active.Add(die);
            }
        }

        /// <summary>Lock a specific die, moving it from Active to Locked.</summary>
        public void LockDie(Die die)
        {
            if (!Active.Contains(die))
                throw new InvalidOperationException("Can only lock dice that are in the Active zone.");
            Active.Remove(die);
            die.IsLocked = true;
            Locked.Add(die);
        }

        /// <summary>Auto-lock all Immediate dice in Active zone.</summary>
        public List<Die> AutoLockImmediateDice()
        {
            var immediateDice = Active.Where(d => d.MustLockImmediately).ToList();
            foreach (var die in immediateDice)
            {
                Active.Remove(die);
                die.IsLocked = true;
                Locked.Add(die);
            }
            return immediateDice;
        }

        /// <summary>Move remaining active dice back to Cup for next roll.</summary>
        public void ReturnActiveToCup()
        {
            var dice = Active.ToList();
            foreach (var die in dice)
            {
                Active.Remove(die);
                Cup.Add(die);
            }
        }

        /// <summary>Collect all dice back to Cup (start of turn).</summary>
        public void CollectAllToCup(SmartList<Die> playerDicePool)
        {
            Cup.Clear();
            Active.Clear();
            Locked.Clear();
            SetAside.Clear();

            // Remove temporary dice
            var temps = Temporary.ToList();
            foreach (var t in temps)
            {
                Temporary.Remove(t);
                playerDicePool.Remove(t);
                t.Destroy();
            }

            foreach (var die in playerDicePool)
            {
                die.IsLocked = false;
                die.TempPipModifier = 0;
                Cup.Add(die);
            }
        }

        /// <summary>Get all locked dice with pip values (for criteria evaluation).</summary>
        public IReadOnlyList<Die> GetLockedDiceWithPips() =>
            Locked.Where(d => d.HasPipValue).ToList().AsReadOnly();

        /// <summary>Get all locked dice (including special faces).</summary>
        public IReadOnlyList<Die> GetAllLockedDice() => Locked.ToList().AsReadOnly();
    }
}
