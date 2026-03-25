using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Dice;
using FotP.Engine.State;

namespace FotP.View
{
    /// <summary>
    /// Displays the player's locked-dice pyramid and their current pip-sum score.
    ///
    /// Locked dice are laid out in a triangular arrangement:
    ///   Row 1 (bottom): up to 3 dice
    ///   Row 2:          up to 2 dice
    ///   Row 3 (top):    1 die
    /// Additional dice beyond 6 overflow into an "extras" row.
    ///
    /// Call <see cref="Bind"/> each time the active player changes.
    /// </summary>
    public class LockedPyramidView : MonoBehaviour
    {
        [Header("UI References")]
        [Tooltip("Label showing the current pip-sum score")]
        public Text scoreLabel;

        [Tooltip("Container rows from bottom to top; index 0 = bottom row")]
        public List<Transform> pyramidRows;

        [Tooltip("Overflow container for more than 6 locked dice")]
        public Transform overflowRow;

        [Header("Prefabs")]
        public DieView diePrefab;

        // -----------------------------------------------------------------------
        // Pyramid layout: max dice per row (bottom → top)
        // -----------------------------------------------------------------------
        private static readonly int[] RowCapacity = { 3, 2, 1 };

        // -----------------------------------------------------------------------
        // Runtime state
        // -----------------------------------------------------------------------

        private readonly List<DieView> _dieViews = new();
        private GameController  _controller;
        private DiceZoneManager _subscribedZones;

        // -----------------------------------------------------------------------
        // Unity lifecycle
        // -----------------------------------------------------------------------

        void OnDestroy()
        {
            UnsubscribeZoneEvents();
        }

        // -----------------------------------------------------------------------
        // Public API
        // -----------------------------------------------------------------------

        /// <summary>Wire this view to the running game and rebuild immediately.</summary>
        public void Bind(GameController controller)
        {
            _controller = controller;
            UnsubscribeZoneEvents();
            Rebuild();
        }

        /// <summary>Rebuild the pyramid display from scratch.</summary>
        public void Rebuild()
        {
            ClearDieViews();

            if (_controller?.Engine == null) return;

            var zones  = _controller.Engine.State.TurnState.Zones;
            var locked = zones.Locked.ToList();

            // Score label
            int score = locked
                .Where(d => d.HasPipValue)
                .Sum(d => d.PipValue);

            if (scoreLabel != null)
                scoreLabel.text = $"Score: {score}";

            // Place dice into pyramid rows
            int placed = 0;
            for (int row = 0; row < RowCapacity.Length && placed < locked.Count; row++)
            {
                int cap     = RowCapacity[row];
                Transform container = (row < pyramidRows?.Count) ? pyramidRows[row] : overflowRow;
                if (container == null) { placed += cap; continue; }

                for (int i = 0; i < cap && placed < locked.Count; i++, placed++)
                {
                    var die = locked[placed];
                    SpawnLockedDie(die, container);
                }
            }

            // Overflow
            for (; placed < locked.Count; placed++)
                SpawnLockedDie(locked[placed], overflowRow != null ? overflowRow : transform);

            SubscribeZoneEvents(zones);
        }

        // -----------------------------------------------------------------------
        // Helpers
        // -----------------------------------------------------------------------

        private void SpawnLockedDie(Die die, Transform container)
        {
            if (diePrefab == null) return;
            var go = Instantiate(diePrefab.gameObject, container);
            var dv = go.GetComponent<DieView>();
            if (dv == null) return;

            dv.Bind(die, _ => { /* locked dice are not interactive */ });
            dv.SetLocked();
            _dieViews.Add(dv);
        }

        private void ClearDieViews()
        {
            foreach (var dv in _dieViews)
                if (dv != null) Destroy(dv.gameObject);
            _dieViews.Clear();
        }

        // -----------------------------------------------------------------------
        // Zone subscriptions
        // -----------------------------------------------------------------------

        private void SubscribeZoneEvents(DiceZoneManager zones)
        {
            _subscribedZones = zones;
            zones.Locked.OnAdded   += OnZoneChanged;
            zones.Locked.OnRemoved += OnZoneChanged;
        }

        private void UnsubscribeZoneEvents()
        {
            if (_subscribedZones == null) return;
            _subscribedZones.Locked.OnAdded   -= OnZoneChanged;
            _subscribedZones.Locked.OnRemoved -= OnZoneChanged;
            _subscribedZones = null;
        }

        private void OnZoneChanged(Die _) =>
            UnityMainThreadDispatcher.Enqueue(Rebuild);
    }
}
