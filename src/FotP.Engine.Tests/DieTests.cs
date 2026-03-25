using System;
using FotP.Engine.Dice;
using Xunit;

namespace FotP.Engine.Tests
{
    public class DieTests
    {
        [Fact]
        public void Standard_Die_Has_Faces_1_Through_6()
        {
            var die = new Die(DieType.Standard);
            var faces = die.GetFaces();
            Assert.Equal(new[] { 1, 2, 3, 4, 5, 6 }, faces);
        }

        [Fact]
        public void Roll_Produces_Valid_Face()
        {
            var rng = new Random(42);
            var die = new Die(DieType.Standard);

            for (int i = 0; i < 100; i++)
            {
                die.Roll(rng);
                Assert.InRange(die.Value, 1, 6);
                Assert.True(die.HasPipValue);
                Assert.Equal(die.Value, die.PipValue);
            }
        }

        [Fact]
        public void Serf_Die_Has_Correct_Faces()
        {
            var die = new Die(DieType.Serf);
            var faces = die.GetFaces();
            Assert.Equal(new[] { 1, 2, 3, 4, 1, 2 }, faces);
            Assert.Equal(4, die.MaxValue);
        }

        [Fact]
        public void Noble_Die_Has_Correct_Faces()
        {
            var die = new Die(DieType.Noble);
            var faces = die.GetFaces();
            Assert.Equal(new[] { 5, 6, 3, 4, 5, 6 }, faces);
            Assert.Equal(6, die.MaxValue);
        }

        [Fact]
        public void Artisan_Die_Star_Face()
        {
            var die = new Die(DieType.Artisan);
            die.SetFaceIndex(0); // Star face
            Assert.True(die.IsStarFace);
            Assert.True(die.HasPipValue); // Star face has pip value 1
            Assert.Equal(1, die.PipValue);
        }

        [Fact]
        public void Intrigue_Die_DoubleStar_Faces()
        {
            var die = new Die(DieType.Intrigue);
            die.SetFaceIndex(0); // Double-star
            Assert.True(die.IsDoubleStarFace);
            Assert.False(die.HasPipValue);
            Assert.Equal(0, die.PipValue);

            die.SetFaceIndex(5); // Also double-star
            Assert.True(die.IsDoubleStarFace);
            Assert.False(die.HasPipValue);
        }

        [Fact]
        public void Voyage_Die_Has_No_Pip_Values()
        {
            var rng = new Random(42);
            var die = new Die(DieType.Voyage);

            for (int i = 0; i < 100; i++)
            {
                die.Roll(rng);
                Assert.False(die.HasPipValue);
                Assert.Equal(0, die.PipValue);
            }
        }

        [Fact]
        public void TempPipModifier_Capped_At_MaxValue()
        {
            var die = new Die(DieType.Standard);
            die.SetValue(6);
            die.TempPipModifier = 5; // Would make it 11
            Assert.Equal(6, die.PipValue); // Capped at 6
        }

        [Fact]
        public void SetValue_Invalid_Throws()
        {
            var die = new Die(DieType.Standard);
            Assert.Throws<ArgumentException>(() => die.SetValue(99));
        }

        [Fact]
        public void Immediate_Die_MustLockImmediately()
        {
            var die = new Die(DieType.Immediate);
            Assert.True(die.MustLockImmediately);

            var standard = new Die(DieType.Standard);
            Assert.False(standard.MustLockImmediately);
        }

        [Fact]
        public void Decree_Die_Star_Face()
        {
            var die = new Die(DieType.Decree);
            die.SetFaceIndex(0); // Star face
            Assert.True(die.IsStarFace);
            Assert.Equal(1, die.PipValue);
        }
    }
}
