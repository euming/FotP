using FotP.Engine.Core;
using Xunit;

namespace FotP.Engine.Tests
{
    public class SmartListTests
    {
        private class TestEntity : GameEntity
        {
            public TestEntity(string name) { EntityName = name; }
        }

        [Fact]
        public void Add_And_Count()
        {
            var list = new SmartList<TestEntity>();
            var e = new TestEntity("A");
            list.Add(e);
            Assert.Equal(1, list.Count);
            Assert.True(list.Contains(e));
        }

        [Fact]
        public void Remove_Entity()
        {
            var list = new SmartList<TestEntity>();
            var e = new TestEntity("A");
            list.Add(e);
            list.Remove(e);
            Assert.Equal(0, list.Count);
            Assert.False(list.Contains(e));
        }

        [Fact]
        public void Add_Duplicate_Ignored()
        {
            var list = new SmartList<TestEntity>();
            var e = new TestEntity("A");
            list.Add(e);
            list.Add(e);
            Assert.Equal(1, list.Count);
        }

        [Fact]
        public void Multi_Membership()
        {
            var list1 = new SmartList<TestEntity>();
            var list2 = new SmartList<TestEntity>();
            var e = new TestEntity("A");

            list1.Add(e);
            list2.Add(e);

            Assert.True(list1.Contains(e));
            Assert.True(list2.Contains(e));
        }

        [Fact]
        public void Cascade_Delete_Removes_From_All_Lists()
        {
            var list1 = new SmartList<TestEntity>();
            var list2 = new SmartList<TestEntity>();
            var list3 = new SmartList<TestEntity>();
            var e = new TestEntity("A");

            list1.Add(e);
            list2.Add(e);
            list3.Add(e);

            e.Destroy();

            Assert.False(list1.Contains(e));
            Assert.False(list2.Contains(e));
            Assert.False(list3.Contains(e));
            Assert.Equal(0, list1.Count);
        }

        [Fact]
        public void Parent_Child_Cascade()
        {
            var list = new SmartList<TestEntity>();
            var parent = new TestEntity("Parent");
            var child = new TestEntity("Child");

            child.SetParent(parent);
            list.Add(parent);
            list.Add(child);

            parent.Destroy();

            Assert.True(child.IsDestroyed);
            Assert.Equal(0, list.Count);
        }

        [Fact]
        public void OnAdded_And_OnRemoved_Events()
        {
            var list = new SmartList<TestEntity>();
            var e = new TestEntity("A");

            TestEntity? addedEntity = null;
            TestEntity? removedEntity = null;
            list.OnAdded += item => addedEntity = item;
            list.OnRemoved += item => removedEntity = item;

            list.Add(e);
            Assert.Equal(e, addedEntity);

            list.Remove(e);
            Assert.Equal(e, removedEntity);
        }

        [Fact]
        public void Clear_Fires_OnRemoved_For_Each()
        {
            var list = new SmartList<TestEntity>();
            var e1 = new TestEntity("A");
            var e2 = new TestEntity("B");
            list.Add(e1);
            list.Add(e2);

            int removedCount = 0;
            list.OnRemoved += _ => removedCount++;

            list.Clear();
            Assert.Equal(2, removedCount);
            Assert.Equal(0, list.Count);
        }

        [Fact]
        public void MoveTo_Transfers_Items()
        {
            var source = new SmartList<TestEntity>();
            var dest = new SmartList<TestEntity>();
            var e1 = new TestEntity("A");
            var e2 = new TestEntity("B");

            source.Add(e1);
            source.Add(e2);

            source.MoveTo(dest);

            Assert.Equal(0, source.Count);
            Assert.Equal(2, dest.Count);
            Assert.True(dest.Contains(e1));
            Assert.True(dest.Contains(e2));
        }

        [Fact]
        public void Destroyed_Entity_Cannot_Be_Added()
        {
            var list = new SmartList<TestEntity>();
            var e = new TestEntity("A");
            e.Destroy();

            Assert.Throws<System.InvalidOperationException>(() => list.Add(e));
        }

        [Fact]
        public void Enumeration_Works()
        {
            var list = new SmartList<TestEntity>();
            var e1 = new TestEntity("A");
            var e2 = new TestEntity("B");
            list.Add(e1);
            list.Add(e2);

            var names = new System.Collections.Generic.List<string?>();
            foreach (var e in list)
                names.Add(e.EntityName);

            Assert.Equal(2, names.Count);
            Assert.Contains("A", names);
            Assert.Contains("B", names);
        }

        [Fact]
        public void Deep_Parent_Child_Cascade()
        {
            var list = new SmartList<TestEntity>();
            var grandparent = new TestEntity("GP");
            var parent = new TestEntity("P");
            var child = new TestEntity("C");

            parent.SetParent(grandparent);
            child.SetParent(parent);

            list.Add(grandparent);
            list.Add(parent);
            list.Add(child);

            grandparent.Destroy();

            Assert.True(parent.IsDestroyed);
            Assert.True(child.IsDestroyed);
            Assert.Equal(0, list.Count);
        }
    }
}
