using System;
using FotP.Engine.Core;

namespace FotP.Engine.Dice
{
    public class Die : GameEntity
    {
        private readonly int[] _faces;

        public DieType DieType { get; }

        /// <summary>Raw face value (may be negative for special faces).</summary>
        public int Value { get; private set; }

        public bool IsLocked { get; set; }
        public bool IsTemporary { get; set; }

        /// <summary>Modifier applied to pip value (e.g., from abilities).</summary>
        public int TempPipModifier { get; set; }

        /// <summary>Index into the _faces array (0-5).</summary>
        public int FaceIndex { get; private set; }

        public Die(DieType type)
        {
            DieType = type;
            _faces = DieFaces.GetFaces(type);
            Value = _faces[0];
            FaceIndex = 0;
            EntityName = type.ToString() + " Die";
        }

        public void Roll(Random rng)
        {
            FaceIndex = rng.Next(_faces.Length);
            Value = _faces[FaceIndex];
        }

        /// <summary>Sets die to a specific face value (must be a valid face).</summary>
        public void SetValue(int value)
        {
            for (int i = 0; i < _faces.Length; i++)
            {
                if (_faces[i] == value)
                {
                    FaceIndex = i;
                    Value = value;
                    return;
                }
            }
            throw new ArgumentException($"Value {value} is not a valid face for {DieType} die.");
        }

        /// <summary>Sets die to a face by index (0-5).</summary>
        public void SetFaceIndex(int index)
        {
            if (index < 0 || index >= _faces.Length)
                throw new ArgumentOutOfRangeException(nameof(index));
            FaceIndex = index;
            Value = _faces[index];
        }

        /// <summary>Whether this die currently shows a face with a pip value.</summary>
        public bool HasPipValue => DieFaces.HasPipValue(Value);

        /// <summary>
        /// The effective pip value of this die, including temp modifier.
        /// Returns 0 if the die has no pip value.
        /// </summary>
        public int PipValue
        {
            get
            {
                if (!HasPipValue) return 0;
                int raw = DieFaces.GetPipValue(Value) + TempPipModifier;
                return Math.Max(0, Math.Min(raw, MaxValue));
            }
        }

        /// <summary>Maximum pip value achievable on this die type.</summary>
        public int MaxValue
        {
            get
            {
                int max = 0;
                foreach (var f in _faces)
                {
                    int pv = DieFaces.GetPipValue(f);
                    if (pv > max) max = pv;
                }
                return max;
            }
        }

        /// <summary>Whether this die is showing a special star (*) face.</summary>
        public bool IsStarFace => Value == DieFaces.StarFace;

        /// <summary>Whether this die is showing a double-star (**) face.</summary>
        public bool IsDoubleStarFace => Value == DieFaces.DoubleStarFace;

        /// <summary>Whether this die must be locked immediately after rolling.</summary>
        public bool MustLockImmediately => DieType == DieType.Immediate;

        public int[] GetFaces() => (int[])_faces.Clone();

        public override string ToString()
        {
            string val = HasPipValue ? PipValue.ToString() : (IsStarFace ? "*" : "**");
            string locked = IsLocked ? " [L]" : "";
            string temp = IsTemporary ? " (temp)" : "";
            return $"{DieType}:{val}{locked}{temp}";
        }
    }
}
