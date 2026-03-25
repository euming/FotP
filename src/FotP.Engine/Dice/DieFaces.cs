using System.Collections.Generic;

namespace FotP.Engine.Dice
{
    /// <summary>
    /// Defines the faces for each die type.
    /// Negative values represent special faces:
    ///   -1 = * (Artisan/Decree star: pip value 1, triggers ability)
    ///   -2 = ** (Intrigue double-star: no pip value, triggers adjust 2)
    ///   -10..-15 = Voyage special faces
    /// </summary>
    public static class DieFaces
    {
        // Voyage face codes
        public const int VoyageAdjust = -10;
        public const int VoyageReroll = -11;
        public const int VoyageDoubleDice = -12;
        public const int VoyageLock = -13;

        public const int StarFace = -1;      // * face (pip value 1)
        public const int DoubleStarFace = -2; // ** face (no pip value)

        private static readonly Dictionary<DieType, int[]> _faces = new()
        {
            [DieType.Standard]  = new[] { 1, 2, 3, 4, 5, 6 },
            [DieType.Immediate] = new[] { 1, 2, 3, 4, 5, 6 },
            [DieType.Serf]      = new[] { 1, 2, 3, 4, 1, 2 },
            [DieType.Noble]     = new[] { 5, 6, 3, 4, 5, 6 },
            [DieType.Artisan]   = new[] { StarFace, 2, 3, 4, 5, 6 },
            [DieType.Intrigue]  = new[] { DoubleStarFace, 2, 3, 4, 5, DoubleStarFace },
            [DieType.Voyage]    = new[] { VoyageAdjust, VoyageReroll, VoyageReroll, VoyageDoubleDice, VoyageLock, VoyageLock },
            [DieType.Decree]    = new[] { StarFace, 2, 3, 4, 5, 6 },
        };

        public static int[] GetFaces(DieType type) => (int[])_faces[type].Clone();

        public static bool IsSpecialFace(int faceValue) => faceValue <= 0;

        /// <summary>
        /// Returns the pip value of a face. Star faces have pip value 1.
        /// Double-star and voyage faces have no pip value (returns 0).
        /// </summary>
        public static int GetPipValue(int faceValue)
        {
            if (faceValue > 0) return faceValue;
            if (faceValue == StarFace) return 1; // * face = pip value 1
            return 0; // ** and voyage faces have no pip value
        }

        public static bool HasPipValue(int faceValue)
        {
            return faceValue > 0 || faceValue == StarFace;
        }
    }
}
