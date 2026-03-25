using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Dice;
using FotP.Engine.State;

namespace FotP.View
{
    /// <summary>
    /// Displays the dice cup for the current player and exposes Roll / End-Turn actions.
    ///
    /// The cup shows how many dice are waiting to be rolled.  The player can:
    ///   • Press Roll — resolves ChooseContinueRolling(true).
    ///   • Press End Turn — resolves ChooseContinueRolling(false).
    ///
    /// Call <see cref="Bind"/> each time the active player changes.
    /// </summary>
    public class DiceCupView : MonoBehaviour
    {
        [Header("UI References")]
        [Tooltip("Label showing number of dice in the cup (e.g. '3 dice')")]
        public Text cupCountLabel;

        [Tooltip("Button to roll remaining cup dice")]
        public Button rollButton;

        [Tooltip("Button to end the turn without rolling further")]
        public Button endTurnButton;

        [Tooltip("Optional label for roll count / status")]
        public Text statusLabel;

        // -----------------------------------------------------------------------
        // Runtime state
        // -----------------------------------------------------------------------

        private GameController   _controller;
        private DiceZoneManager  _subscribedZones;

        // -----------------------------------------------------------------------
        // Unity lifecycle
        // -----------------------------------------------------------------------

        void Awake()
        {
            if (rollButton    != null) rollButton.onClick.AddListener(OnRoll);
            if (endTurnButton != null) endTurnButton.onClick.AddListener(OnEndTurn);
        }

        void OnDestroy()
        {
            if (rollButton    != null) rollButton.onClick.RemoveListener(OnRoll);
            if (endTurnButton != null) endTurnButton.onClick.RemoveListener(OnEndTurn);
            UnsubscribeZoneEvents();
        }

        // -----------------------------------------------------------------------
        // Public API
        // -----------------------------------------------------------------------

        /// <summary>Wire this view to the running game.</summary>
        public void Bind(GameController controller)
        {
            _controller = controller;
            UnsubscribeZoneEvents();
            Refresh();
        }

        /// <summary>Update labels and button states from current engine state.</summary>
        public void Refresh()
        {
            if (_controller?.Engine == null)
            {
                SetInteractable(false);
                return;
            }

            var zones    = _controller.Engine.State.TurnState.Zones;
            int cupCount = zones.Cup.Count;
            int rollCount = _controller.Engine.State.TurnState.RollCount;

            if (cupCountLabel != null)
                cupCountLabel.text = cupCount == 1 ? "1 die" : $"{cupCount} dice";

            if (statusLabel != null)
                statusLabel.text = rollCount == 0 ? "First roll" : $"Roll #{rollCount + 1}";

            // Buttons are only meaningful when ChooseContinueRolling is pending.
            // We enable them conservatively; they become no-ops if no TCS is waiting.
            bool hasCupDice = cupCount > 0;
            if (rollButton    != null) rollButton.interactable    = hasCupDice;
            if (endTurnButton != null) endTurnButton.interactable = true;

            SubscribeZoneEvents(zones);
        }

        // -----------------------------------------------------------------------
        // Button handlers
        // -----------------------------------------------------------------------

        private void OnRoll()
        {
            _controller?.CurrentInput?.ResolveContinueRolling(true);
        }

        private void OnEndTurn()
        {
            _controller?.CurrentInput?.ResolveContinueRolling(false);
        }

        // -----------------------------------------------------------------------
        // Zone subscriptions
        // -----------------------------------------------------------------------

        private void SubscribeZoneEvents(DiceZoneManager zones)
        {
            _subscribedZones = zones;
            zones.Cup.OnAdded   += OnZoneChanged;
            zones.Cup.OnRemoved += OnZoneChanged;
        }

        private void UnsubscribeZoneEvents()
        {
            if (_subscribedZones == null) return;
            _subscribedZones.Cup.OnAdded   -= OnZoneChanged;
            _subscribedZones.Cup.OnRemoved -= OnZoneChanged;
            _subscribedZones = null;
        }

        private void OnZoneChanged(Die _) =>
            UnityMainThreadDispatcher.Enqueue(Refresh);

        private void SetInteractable(bool on)
        {
            if (rollButton    != null) rollButton.interactable    = on;
            if (endTurnButton != null) endTurnButton.interactable = on;
        }
    }
}
