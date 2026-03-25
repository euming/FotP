using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Dice;
using FotP.Engine.State;

namespace FotP.View
{
    /// <summary>
    /// Displays the active (just-rolled) dice for the current player.
    /// Players tap individual dice to select them for locking, then press Confirm.
    ///
    /// Workflow:
    ///   1. GameController sets <see cref="Bind"/> once per turn when ChooseDiceToLock fires.
    ///   2. Player taps dice to toggle selection.
    ///   3. Player presses Confirm — resolves the engine's ChooseDiceToLock.
    /// </summary>
    public class ActiveDiceAreaView : MonoBehaviour
    {
        [Header("UI References")]
        [Tooltip("Container with a HorizontalLayoutGroup for die children")]
        public Transform dieContainer;

        [Tooltip("Button the player presses to confirm their lock selection")]
        public Button confirmButton;

        [Tooltip("Optional label showing instruction text")]
        public Text promptLabel;

        [Header("Prefabs")]
        public DieView diePrefab;

        // -----------------------------------------------------------------------
        // Runtime state
        // -----------------------------------------------------------------------

        private readonly List<DieView> _dieViews = new();
        private UnityPlayerInput _input;
        private GameController   _controller;

        // -----------------------------------------------------------------------
        // Unity lifecycle
        // -----------------------------------------------------------------------

        void Awake()
        {
            if (confirmButton != null)
                confirmButton.onClick.AddListener(OnConfirm);
        }

        void OnDestroy()
        {
            if (confirmButton != null)
                confirmButton.onClick.RemoveListener(OnConfirm);
            UnsubscribeZoneEvents();
        }

        // -----------------------------------------------------------------------
        // Public API
        // -----------------------------------------------------------------------

        /// <summary>
        /// Called once per frame from a supervisor (e.g. a turn coordinator) to
        /// wire this view to the correct player's input and dice zones.
        /// </summary>
        public void Bind(GameController controller)
        {
            _controller = controller;
            UnsubscribeZoneEvents();
            Refresh();
        }

        /// <summary>Rebuild the die views from the current active zone.</summary>
        public void Refresh()
        {
            ClearDieViews();

            if (_controller?.Engine == null) return;

            var zones = _controller.Engine.State.TurnState.Zones;
            var active = zones.Active.ToList();

            if (promptLabel != null)
                promptLabel.text = active.Count == 0
                    ? "No active dice"
                    : "Select dice to lock, then confirm.";

            foreach (var die in active)
            {
                if (diePrefab == null || dieContainer == null) break;
                var go = Instantiate(diePrefab.gameObject, dieContainer);
                var dv = go.GetComponent<DieView>();
                if (dv != null)
                {
                    dv.Bind(die, OnDieClicked);

                    // Immediate dice are pre-selected (must lock)
                    if (die.MustLockImmediately)
                        dv.SetSelected(true);

                    _dieViews.Add(dv);
                }
            }

            SubscribeZoneEvents(zones);

            if (confirmButton != null)
                confirmButton.interactable = active.Count > 0;
        }

        // -----------------------------------------------------------------------
        // Internal
        // -----------------------------------------------------------------------

        private void OnDieClicked(DieView dv)
        {
            // Immediate dice cannot be deselected
            if (dv.Die.MustLockImmediately) return;
            dv.SetSelected(!dv.IsSelected);
        }

        private void OnConfirm()
        {
            var input = _controller?.CurrentInput;
            if (input == null) return;

            var selected = _dieViews
                .Where(dv => dv.IsSelected)
                .Select(dv => dv.Die)
                .ToList();

            input.ResolveDiceToLock(selected);

            // Hide/clear until next roll
            ClearDieViews();
            if (confirmButton != null)
                confirmButton.interactable = false;
        }

        // -----------------------------------------------------------------------
        // Zone event subscriptions — auto-refresh when active dice change
        // -----------------------------------------------------------------------

        private DiceZoneManager _subscribedZones;

        private void SubscribeZoneEvents(DiceZoneManager zones)
        {
            _subscribedZones = zones;
            zones.Active.OnAdded   += OnZoneChanged;
            zones.Active.OnRemoved += OnZoneChanged;
        }

        private void UnsubscribeZoneEvents()
        {
            if (_subscribedZones == null) return;
            _subscribedZones.Active.OnAdded   -= OnZoneChanged;
            _subscribedZones.Active.OnRemoved -= OnZoneChanged;
            _subscribedZones = null;
        }

        private void OnZoneChanged(Die _)
        {
            // SmartList events fire on the engine thread; marshal to main thread.
            UnityMainThreadDispatcher.Enqueue(Refresh);
        }

        private void ClearDieViews()
        {
            foreach (var dv in _dieViews)
                if (dv != null) Destroy(dv.gameObject);
            _dieViews.Clear();
        }
    }
}
