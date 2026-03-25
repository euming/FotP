using System;
using System.Collections.Generic;

namespace FotP.Engine.Core
{
    /// <summary>
    /// Base class for all game objects. Tracks SmartList memberships and supports
    /// parent-child ownership with cascade delete.
    /// </summary>
    public class GameEntity : IDisposable
    {
        private readonly List<ISmartListInternal> _memberships = new();
        private readonly List<GameEntity> _children = new();
        private GameEntity? _parent;
        private bool _disposed;

        public string? EntityName { get; set; }
        public bool IsDestroyed => _disposed;

        internal void RegisterMembership(ISmartListInternal list)
        {
            if (!_disposed && !_memberships.Contains(list))
                _memberships.Add(list);
        }

        internal void UnregisterMembership(ISmartListInternal list)
        {
            _memberships.Remove(list);
        }

        /// <summary>
        /// Establishes a parent-child relationship. Destroying the parent cascades to children.
        /// </summary>
        public void SetParent(GameEntity parent)
        {
            if (_parent == parent) return;
            _parent?.RemoveChild(this);
            _parent = parent;
            parent.AddChild(this);
        }

        public void AddChild(GameEntity child)
        {
            if (!_children.Contains(child))
            {
                _children.Add(child);
                if (child._parent != this)
                    child._parent = this;
            }
        }

        public void RemoveChild(GameEntity child)
        {
            _children.Remove(child);
            if (child._parent == this)
                child._parent = null;
        }

        public IReadOnlyList<GameEntity> Children => _children.AsReadOnly();

        /// <summary>
        /// Destroys this entity: removes from all SmartLists, cascades to children.
        /// </summary>
        public void Destroy()
        {
            if (_disposed) return;
            _disposed = true;

            // Cascade to children (copy list since children modify it)
            var childrenCopy = new List<GameEntity>(_children);
            foreach (var child in childrenCopy)
                child.Destroy();
            _children.Clear();

            // Remove from all SmartLists
            var membershipsCopy = new List<ISmartListInternal>(_memberships);
            foreach (var list in membershipsCopy)
                list.RemoveEntity(this);
            _memberships.Clear();

            // Detach from parent
            _parent?.RemoveChild(this);
            _parent = null;

            OnDestroyed();
        }

        protected virtual void OnDestroyed() { }

        public void Dispose()
        {
            Destroy();
            GC.SuppressFinalize(this);
        }
    }

    /// <summary>
    /// Internal interface allowing GameEntity to remove itself from any SmartList without knowing T.
    /// </summary>
    internal interface ISmartListInternal
    {
        void RemoveEntity(GameEntity entity);
    }
}
