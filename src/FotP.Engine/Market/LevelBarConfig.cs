using System.Collections.Generic;
using FotP.Engine.Criteria;

namespace FotP.Engine.Market
{
    /// <summary>
    /// Which side of the level bar is active for a given level.
    /// </summary>
    public enum BarSide { A, B }

    /// <summary>
    /// The claim criteria for one side of one level bar.
    /// Each entry corresponds to one slot (position) on the bar.
    /// Slots are ordered from position 0 (leftmost / used in 2-player) to
    /// position N-1 (rightmost / only used in N+2-player games).
    /// The active player-count slots are selected at setup time.
    /// </summary>
    public class LevelBarSide
    {
        public int Level { get; }
        public BarSide Side { get; }
        /// <summary>One criterion per bar slot, ordered left-to-right.</summary>
        public IReadOnlyList<Criterion> SlotCriteria { get; }

        public LevelBarSide(int level, BarSide side, IReadOnlyList<Criterion> slotCriteria)
        {
            Level = level;
            Side = side;
            SlotCriteria = slotCriteria;
        }
    }

    /// <summary>
    /// Canonical A-side and B-side configuration for all five level bars (3-7),
    /// transcribed from FotP_Online_rules_v20 Level Bars reference card (page 5).
    ///
    /// Slot ordering within each bar matches the physical bar's left-to-right layout.
    /// For a P-player game, use slots [0..P-1].
    ///
    /// IMPORTANT: The Queen bar (level 7) has only one slot; its "criteria" is
    /// a minimum sum gate (A=40+, B=43+). The Queen tile itself uses SumGreaterEqual.
    /// </summary>
    public static class LevelBarConfig
    {
        // ------------------------------------------------------------------
        // Level 3
        // A-side: Three of a Kind | Pair | All Even | All Odd | All Dice 2-4
        // B-side: Three of a Kind | All Dice 1-4 | All Even | All Odd | Sum>=11
        // ------------------------------------------------------------------
        public static readonly LevelBarSide Level3A = new(3, BarSide.A, new Criterion[]
        {
            new NOfAKind(3),             // slot 0
            new NOfAKind(2),             // slot 1 (Pair)
            new AllEven(),               // slot 2
            new AllOdd(),                // slot 3
            new AllDiceInRange(2, 4),    // slot 4
        });

        public static readonly LevelBarSide Level3B = new(3, BarSide.B, new Criterion[]
        {
            new NOfAKind(3),             // slot 0
            new AllDiceInRange(1, 4),    // slot 1 (B-side: 1-4 instead of Pair)
            new AllEven(),               // slot 2
            new AllOdd(),                // slot 3
            new SumGreaterEqual(11),     // slot 4 (B-side: sum gate instead of range)
        });

        // ------------------------------------------------------------------
        // Level 4
        // A-side: Four of a Kind | Straight(4) | All Dice 2-5 | All Different | Sum>=20
        // B-side: Four of a Kind | Straight(4) | Two Pairs   | Pair(6)+Pair(1) | Sum>=20
        // ------------------------------------------------------------------
        public static readonly LevelBarSide Level4A = new(4, BarSide.A, new Criterion[]
        {
            new NOfAKind(4),             // slot 0
            new Straight(4),             // slot 1 (1234 / 2345 / 3456)
            new AllDiceInRange(2, 5),    // slot 2
            new AllDifferent(),          // slot 3
            new SumGreaterEqual(20),     // slot 4
        });

        public static readonly LevelBarSide Level4B = new(4, BarSide.B, new Criterion[]
        {
            new NOfAKind(4),             // slot 0
            new Straight(4),             // slot 1
            new TwoPairs(),              // slot 2 (B-side: Two Pairs instead of range)
            new CompoundCriteria(new PairOfValue(6), new PairOfValue(1)), // slot 3: Pair of 6s & Pair of 1s
            new SumGreaterEqual(20),     // slot 4
        });

        // ------------------------------------------------------------------
        // Level 5
        // A-side: Five of a Kind | Straight(5) | All Dice 3-6 | Full House | Sum>=25
        // B-side: Five of a Kind | Straight(5) | Three 6's   | Two 3-of-a-Kinds | Sum>=25
        // ------------------------------------------------------------------
        public static readonly LevelBarSide Level5A = new(5, BarSide.A, new Criterion[]
        {
            new NOfAKind(5),             // slot 0
            new Straight(5),             // slot 1 (12345 / 23456)
            new AllDiceInRange(3, 6),    // slot 2
            new FullHouse(),             // slot 3
            new SumGreaterEqual(25),     // slot 4
        });

        public static readonly LevelBarSide Level5B = new(5, BarSide.B, new Criterion[]
        {
            new NOfAKind(5),             // slot 0
            new Straight(5),             // slot 1
            new NOfValue(3, 6),          // slot 2 (B-side: Three 6's instead of range)
            new TwoNOfAKind(3),          // slot 3 (B-side: Two Three-of-a-Kinds instead of Full House)
            new SumGreaterEqual(25),     // slot 4
        });

        // ------------------------------------------------------------------
        // Level 6
        // A-side: Six of a Kind | Three Pairs | All Dice>=5 | Straight(6) | Sum>=35
        // B-side: Six of a Kind | Straight(6) | NOfAKind(4)+NOfValue(3,3) | Pair+NOfAKind(4) | Sum>=35
        //
        // NOTE: The A-side list from rules also mentions "Four of a Kind and Three 3s"
        // and "Pair and Four of a Kind" as additional slots (for 6-7 player variants).
        // For 2-4 player games slots 0-3 are used.
        // ------------------------------------------------------------------
        public static readonly LevelBarSide Level6A = new(6, BarSide.A, new Criterion[]
        {
            new NOfAKind(6),             // slot 0
            new ThreePairs(),            // slot 1
            new AllDiceInRange(5, 6),    // slot 2 (All Dice >= 5)
            new Straight(6),             // slot 3 (1-2-3-4-5-6 full straight)
            new SumGreaterEqual(35),     // slot 4
        });

        public static readonly LevelBarSide Level6B = new(6, BarSide.B, new Criterion[]
        {
            new NOfAKind(6),             // slot 0
            new Straight(6),             // slot 1 (B-side: straight comes before pairs)
            new CompoundCriteria(new NOfAKind(4), new NOfValue(3, 3)), // slot 2: 4-of-a-kind + Three 3s
            new CompoundCriteria(new NOfAKind(2), new NOfAKind(4)),    // slot 3: Pair + Four of a Kind
            new SumGreaterEqual(35),     // slot 4
        });

        // ------------------------------------------------------------------
        // Level 7 (Queen only — one slot)
        // A-side: Sum >= 40  (first-game threshold; only Queen tile exists here)
        // B-side: Sum >= 43
        //
        // Additional compound criteria listed in rules for later games / higher
        // player counts (e.g., 3-of-a-kind + Two Pairs, Pair + Five-of-a-kind, etc.)
        // are catalogued here as extra slots and are not used in 2-4 player setups.
        // ------------------------------------------------------------------
        public static readonly LevelBarSide Level7A = new(7, BarSide.A, new Criterion[]
        {
            new SumGreaterEqual(40),                                           // slot 0 (Queen)
            new CompoundCriteria(new NOfAKind(3), new TwoPairs()),             // slot 1 (3-of-a-kind + Two Pairs)
            new CompoundCriteria(new NOfAKind(2), new NOfAKind(5)),            // slot 2 (Pair + Five-of-a-kind)
            new CompoundCriteria(new NOfAKind(4), new PairOfValue(6)),         // slot 3 (4-of-a-kind + Pair of 6s)
            new CompoundCriteria(new NOfAKind(3), new NOfAKind(4)),            // slot 4 (3-of-a-kind + 4-of-a-kind)
        });

        public static readonly LevelBarSide Level7B = new(7, BarSide.B, new Criterion[]
        {
            new SumGreaterEqual(43),                                           // slot 0 (Queen, B-side threshold)
            new CompoundCriteria(new NOfAKind(4), new NOfValue(3, 1)),         // slot 1 (4-of-a-kind + Three 1s)
            new CompoundCriteria(new NOfAKind(2), new NOfAKind(5)),            // slot 2 (Pair + Five-of-a-kind)
            new CompoundCriteria(new NOfAKind(4), new PairOfValue(6)),         // slot 3 (4-of-a-kind + Pair of 6s)
            new CompoundCriteria(new NOfAKind(3), new NOfAKind(4)),            // slot 4 (3-of-a-kind + 4-of-a-kind)
        });

        /// <summary>
        /// Returns the correct LevelBarSide for a given level and side.
        /// </summary>
        public static LevelBarSide Get(int level, BarSide side)
        {
            return (level, side) switch
            {
                (3, BarSide.A) => Level3A,
                (3, BarSide.B) => Level3B,
                (4, BarSide.A) => Level4A,
                (4, BarSide.B) => Level4B,
                (5, BarSide.A) => Level5A,
                (5, BarSide.B) => Level5B,
                (6, BarSide.A) => Level6A,
                (6, BarSide.B) => Level6B,
                (7, BarSide.A) => Level7A,
                (7, BarSide.B) => Level7B,
                _ => throw new System.ArgumentOutOfRangeException($"No bar config for level {level} side {side}")
            };
        }
    }
}
