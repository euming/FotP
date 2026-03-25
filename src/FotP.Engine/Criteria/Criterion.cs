using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Dice;

namespace FotP.Engine.Criteria
{
    /// <summary>
    /// Abstract base for tile claim criteria. Only considers dice with pip values.
    /// </summary>
    public abstract class Criterion
    {
        public abstract bool Evaluate(IReadOnlyList<Die> lockedDice);

        /// <summary>Filters to only dice that have pip values.</summary>
        protected static List<int> GetPipValues(IReadOnlyList<Die> dice)
        {
            return dice.Where(d => d.HasPipValue).Select(d => d.PipValue).ToList();
        }

        public abstract string Description { get; }
    }

    public class NOfAKind : Criterion
    {
        private readonly int _n;
        public NOfAKind(int n) { _n = n; }

        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice);
            if (pips.Count < _n) return false;
            return pips.GroupBy(v => v).Any(g => g.Count() >= _n);
        }

        public override string Description => $"{_n} of a Kind";
    }

    public class Straight : Criterion
    {
        private readonly int _length;
        public Straight(int length) { _length = length; }

        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice).Distinct().OrderBy(v => v).ToList();
            if (pips.Count < _length) return false;

            int consecutive = 1;
            for (int i = 1; i < pips.Count; i++)
            {
                if (pips[i] == pips[i - 1] + 1)
                {
                    consecutive++;
                    if (consecutive >= _length) return true;
                }
                else
                {
                    consecutive = 1;
                }
            }
            return consecutive >= _length;
        }

        public override string Description => $"Straight of {_length}";
    }

    public class SumGreaterEqual : Criterion
    {
        private readonly int _threshold;
        public SumGreaterEqual(int threshold) { _threshold = threshold; }

        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice);
            return pips.Sum() >= _threshold;
        }

        public override string Description => $"Sum >= {_threshold}";
    }

    public class AllDifferent : Criterion
    {
        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice);
            return pips.Count > 0 && pips.Distinct().Count() == pips.Count;
        }

        public override string Description => "All Different";
    }

    public class AllEven : Criterion
    {
        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice);
            return pips.Count > 0 && pips.All(v => v % 2 == 0);
        }

        public override string Description => "All Even";
    }

    public class AllOdd : Criterion
    {
        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice);
            return pips.Count > 0 && pips.All(v => v % 2 != 0);
        }

        public override string Description => "All Odd";
    }

    public class AllDiceInRange : Criterion
    {
        private readonly int _min, _max;
        public AllDiceInRange(int min, int max) { _min = min; _max = max; }

        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice);
            return pips.Count > 0 && pips.All(v => v >= _min && v <= _max);
        }

        public override string Description => $"All dice {_min}-{_max}";
    }

    public class PairOfValue : Criterion
    {
        private readonly int _value;
        public PairOfValue(int value) { _value = value; }

        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice);
            return pips.Count(v => v == _value) >= 2;
        }

        public override string Description => $"Pair of {_value}s";
    }

    public class TwoPairs : Criterion
    {
        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice);
            return pips.GroupBy(v => v).Count(g => g.Count() >= 2) >= 2;
        }

        public override string Description => "Two Pairs";
    }

    public class ThreePairs : Criterion
    {
        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice);
            return pips.GroupBy(v => v).Count(g => g.Count() >= 2) >= 3;
        }

        public override string Description => "Three Pairs";
    }

    public class FullHouse : Criterion
    {
        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice);
            var groups = pips.GroupBy(v => v).Select(g => g.Count()).OrderByDescending(c => c).ToList();
            // Must have at least 2 groups; largest group exactly 3, second group exactly 2
            return groups.Count >= 2 && groups[0] == 3 && groups[1] == 2;
        }

        public override string Description => "Full House";
    }

    public class CompoundCriteria : Criterion
    {
        private readonly Criterion[] _criteria;
        public CompoundCriteria(params Criterion[] criteria) { _criteria = criteria; }

        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            return _criteria.All(c => c.Evaluate(lockedDice));
        }

        public override string Description => string.Join(" + ", _criteria.Select(c => c.Description));
    }

    /// <summary>N dice all showing a specific pip value (e.g., Three 6's = NOfValue(3,6)).</summary>
    public class NOfValue : Criterion
    {
        private readonly int _n, _value;
        public NOfValue(int n, int value) { _n = n; _value = value; }

        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice);
            return pips.Count(v => v == _value) >= _n;
        }

        public override string Description => $"{_n} of {_value}s";
    }

    /// <summary>Two separate N-of-a-kind groups (e.g., Two Three-of-a-Kinds = TwoNOfAKind(3)).</summary>
    public class TwoNOfAKind : Criterion
    {
        private readonly int _n;
        public TwoNOfAKind(int n) { _n = n; }

        public override bool Evaluate(IReadOnlyList<Die> lockedDice)
        {
            var pips = GetPipValues(lockedDice);
            return pips.GroupBy(v => v).Count(g => g.Count() >= _n) >= 2;
        }

        public override string Description => $"Two {_n}-of-a-Kinds";
    }
}
