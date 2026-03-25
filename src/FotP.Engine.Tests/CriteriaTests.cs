using System.Collections.Generic;
using FotP.Engine.Criteria;
using FotP.Engine.Dice;
using Xunit;

namespace FotP.Engine.Tests
{
    public class CriteriaTests
    {
        private static List<Die> MakeDice(params int[] values)
        {
            var dice = new List<Die>();
            foreach (var v in values)
            {
                var die = new Die(DieType.Standard);
                die.SetValue(v);
                die.IsLocked = true;
                dice.Add(die);
            }
            return dice;
        }

        [Fact]
        public void NOfAKind_Passes_With_Enough()
        {
            var criterion = new NOfAKind(3);
            Assert.True(criterion.Evaluate(MakeDice(3, 3, 3, 1, 2)));
            Assert.False(criterion.Evaluate(MakeDice(3, 3, 1, 2, 4)));
        }

        [Fact]
        public void NOfAKind_2_Pair()
        {
            var criterion = new NOfAKind(2);
            Assert.True(criterion.Evaluate(MakeDice(4, 4)));
            Assert.False(criterion.Evaluate(MakeDice(1, 2, 3)));
        }

        [Fact]
        public void Straight_Passes()
        {
            var criterion = new Straight(4);
            Assert.True(criterion.Evaluate(MakeDice(2, 3, 4, 5)));
            Assert.True(criterion.Evaluate(MakeDice(1, 2, 3, 4, 6)));
            Assert.False(criterion.Evaluate(MakeDice(1, 2, 4, 6)));
        }

        [Fact]
        public void Straight_3()
        {
            var criterion = new Straight(3);
            Assert.True(criterion.Evaluate(MakeDice(1, 2, 3)));
            Assert.True(criterion.Evaluate(MakeDice(4, 5, 6)));
            Assert.False(criterion.Evaluate(MakeDice(1, 3, 5)));
        }

        [Fact]
        public void SumGreaterEqual_Passes()
        {
            var criterion = new SumGreaterEqual(10);
            Assert.True(criterion.Evaluate(MakeDice(4, 3, 3)));
            Assert.True(criterion.Evaluate(MakeDice(5, 5)));
            Assert.False(criterion.Evaluate(MakeDice(3, 3, 3)));
        }

        [Fact]
        public void AllDifferent_Passes()
        {
            var criterion = new AllDifferent();
            Assert.True(criterion.Evaluate(MakeDice(1, 2, 3, 4)));
            Assert.False(criterion.Evaluate(MakeDice(1, 2, 2, 4)));
        }

        [Fact]
        public void AllEven_Passes()
        {
            var criterion = new AllEven();
            Assert.True(criterion.Evaluate(MakeDice(2, 4, 6)));
            Assert.False(criterion.Evaluate(MakeDice(2, 3, 6)));
        }

        [Fact]
        public void AllOdd_Passes()
        {
            var criterion = new AllOdd();
            Assert.True(criterion.Evaluate(MakeDice(1, 3, 5)));
            Assert.False(criterion.Evaluate(MakeDice(1, 2, 5)));
        }

        [Fact]
        public void AllDiceInRange_Passes()
        {
            var criterion = new AllDiceInRange(3, 5);
            Assert.True(criterion.Evaluate(MakeDice(3, 4, 5)));
            Assert.False(criterion.Evaluate(MakeDice(2, 4, 5)));
        }

        [Fact]
        public void PairOfValue_Passes()
        {
            var criterion = new PairOfValue(5);
            Assert.True(criterion.Evaluate(MakeDice(5, 5, 1)));
            Assert.False(criterion.Evaluate(MakeDice(5, 1, 2)));
        }

        [Fact]
        public void TwoPairs_Passes()
        {
            var criterion = new TwoPairs();
            Assert.True(criterion.Evaluate(MakeDice(2, 2, 5, 5)));
            Assert.False(criterion.Evaluate(MakeDice(2, 2, 3, 5)));
        }

        [Fact]
        public void ThreePairs_Passes()
        {
            var criterion = new ThreePairs();
            Assert.True(criterion.Evaluate(MakeDice(1, 1, 3, 3, 5, 5)));
            Assert.False(criterion.Evaluate(MakeDice(1, 1, 3, 3, 5, 6)));
        }

        [Fact]
        public void FullHouse_Passes()
        {
            var criterion = new FullHouse();
            Assert.True(criterion.Evaluate(MakeDice(3, 3, 3, 5, 5)));
            Assert.False(criterion.Evaluate(MakeDice(3, 3, 3, 3, 5)));
            Assert.False(criterion.Evaluate(MakeDice(1, 2, 3, 4, 5)));
        }

        [Fact]
        public void CompoundCriteria_All_Must_Pass()
        {
            var criterion = new CompoundCriteria(new NOfAKind(2), new SumGreaterEqual(8));
            Assert.True(criterion.Evaluate(MakeDice(4, 4, 1))); // pair + sum 9
            Assert.False(criterion.Evaluate(MakeDice(2, 2, 1))); // pair but sum 5
        }

        [Fact]
        public void Special_Faces_Excluded_From_Pip_Evaluation()
        {
            // Create an Intrigue die showing **
            var dice = new List<Die>();
            var intrigue = new Die(DieType.Intrigue);
            intrigue.SetFaceIndex(0); // double-star, no pip value
            intrigue.IsLocked = true;
            dice.Add(intrigue);

            var standard = new Die(DieType.Standard);
            standard.SetValue(5);
            standard.IsLocked = true;
            dice.Add(standard);

            // NOfAKind(2) should fail - only one die with pip value
            var criterion = new NOfAKind(2);
            Assert.False(criterion.Evaluate(dice));
        }
    }
}
