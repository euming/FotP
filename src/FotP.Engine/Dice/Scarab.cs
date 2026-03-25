using System;
using FotP.Engine.Core;

namespace FotP.Engine.Dice
{
    public class Scarab : GameEntity
    {
        public ScarabType Type { get; }

        public Scarab(ScarabType type)
        {
            Type = type;
            EntityName = type + " Scarab";
        }

        /// <summary>
        /// Apply this scarab's effect.
        /// </summary>
        public Die? Apply(Die? target, Random rng, SmartList<Die>? cupList = null)
        {
            switch (Type)
            {
                case ScarabType.Reroll:
                    if (target == null) throw new ArgumentNullException(nameof(target));
                    target.Roll(rng);
                    return null;

                case ScarabType.Pip:
                    if (target == null) throw new ArgumentNullException(nameof(target));
                    if (target.HasPipValue && target.PipValue < target.MaxValue)
                        target.TempPipModifier++;
                    return null;

                case ScarabType.Die:
                    var tempDie = new Die(DieType.Standard) { IsTemporary = true };
                    tempDie.Roll(rng);
                    cupList?.Add(tempDie);
                    return tempDie;

                default:
                    throw new InvalidOperationException($"Unknown scarab type: {Type}");
            }
        }

        public override string ToString() => $"Scarab({Type})";
    }
}
