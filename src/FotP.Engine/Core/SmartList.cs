using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace FotP.Engine.Core
{
    /// <summary>
    /// Observable generic container supporting multi-membership and cascade delete.
    /// Objects can belong to multiple SmartLists simultaneously.
    /// </summary>
    public class SmartList<T> : ISmartListInternal, IReadOnlyList<T> where T : GameEntity
    {
        private readonly List<T> _items = new();

        public event Action<T>? OnAdded;
        public event Action<T>? OnRemoved;

        public int Count => _items.Count;

        public T this[int index] => _items[index];

        public bool Contains(T item) => _items.Contains(item);

        public void Add(T item)
        {
            if (item.IsDestroyed)
                throw new InvalidOperationException("Cannot add a destroyed entity to a SmartList.");
            if (_items.Contains(item)) return;
            _items.Add(item);
            item.RegisterMembership(this);
            OnAdded?.Invoke(item);
        }

        public bool Remove(T item)
        {
            if (!_items.Remove(item)) return false;
            item.UnregisterMembership(this);
            OnRemoved?.Invoke(item);
            return true;
        }

        public void Clear()
        {
            var copy = new List<T>(_items);
            _items.Clear();
            foreach (var item in copy)
            {
                item.UnregisterMembership(this);
                OnRemoved?.Invoke(item);
            }
        }

        /// <summary>
        /// Moves all items from this list to another SmartList.
        /// </summary>
        public void MoveTo(SmartList<T> target)
        {
            var copy = new List<T>(_items);
            Clear();
            foreach (var item in copy)
                target.Add(item);
        }

        /// <summary>
        /// Removes and returns the first item matching the predicate.
        /// </summary>
        public T? RemoveFirst(Func<T, bool> predicate)
        {
            var item = _items.FirstOrDefault(predicate);
            if (item != null) Remove(item);
            return item;
        }

        public List<T> Where(Func<T, bool> predicate) => _items.Where(predicate).ToList();

        void ISmartListInternal.RemoveEntity(GameEntity entity)
        {
            if (entity is T typed)
            {
                _items.Remove(typed);
                OnRemoved?.Invoke(typed);
            }
        }

        public IEnumerator<T> GetEnumerator() => _items.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public List<T> ToList() => new List<T>(_items);
    }
}
