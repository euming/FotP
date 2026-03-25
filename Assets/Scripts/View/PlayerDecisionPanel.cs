using UnityEngine;

namespace FotP.View
{
    /// <summary>
    /// Master coordinator that activates exactly one decision sub-panel based on
    /// what the engine is currently awaiting from the active player.
    ///
    /// Existing views handle the two core decisions:
    ///   - <see cref="ActiveDiceAreaView"/>: ChooseDiceToLock
    ///   - <see cref="DiceCupView"/>: ChooseContinueRolling
    ///
    /// This panel handles the remaining decisions that require separate UI:
    ///   - <see cref="TileChoicePanel"/>: ChooseTileToClaim / ChooseTile
    ///   - <see cref="DieChoicePanel"/>:  ChooseDie / ChooseMultipleDice / ChoosePipValue
    ///   - <see cref="YesNoPanel"/>:      ChooseYesNo / ChooseUseAbility
    ///   - <see cref="ScarabChoicePanel"/>: ChooseScarab
    ///   - <see cref="PlayerChoicePanel"/>: ChoosePlayer
    ///
    /// Place this MonoBehaviour on a Canvas root.  Assign sub-panel references in
    /// the Inspector.  All sub-panels start hidden; this coordinator shows/hides
    /// them as the engine transitions between decisions.
    /// </summary>
    public class PlayerDecisionPanel : MonoBehaviour
    {
        // ── Engine access ──────────────────────────────────────────────────────

        [Header("Engine")]
        [Tooltip("Provides access to active player inputs.")]
        public GameController gameController;

        // ── Existing core views (also managed here for show/hide) ─────────────

        [Header("Core Views")]
        public ActiveDiceAreaView activeDiceAreaView;
        public DiceCupView        diceCupView;

        // ── Decision-specific sub-panels ──────────────────────────────────────

        [Header("Decision Sub-panels")]
        public TileChoicePanel   tileChoicePanel;
        public DieChoicePanel    dieChoicePanel;
        public YesNoPanel        yesNoPanel;
        public ScarabChoicePanel scarabChoicePanel;
        public PlayerChoicePanel playerChoicePanel;

        // ── Internal ──────────────────────────────────────────────────────────

        private UnityPlayerInput _lastInput;
        private bool             _panelOpen;

        // ── Unity lifecycle ───────────────────────────────────────────────────

        private void Awake()
        {
            if (gameController == null)
                gameController = FindObjectOfType<GameController>();
        }

        private void Update()
        {
            if (gameController == null) return;

            var input = gameController.CurrentInput;

            // When the active player changes, close any open overlay panel.
            if (input != _lastInput)
            {
                _lastInput = input;
                CloseOverlayPanels();
            }

            if (input == null) return;

            // Only attempt to open a panel when none is currently visible.
            if (!_panelOpen)
                TryOpenPanel(input);
        }

        // ── Private ───────────────────────────────────────────────────────────

        private void TryOpenPanel(UnityPlayerInput input)
        {
            // Priority matches the typical engine decision order.

            if (input.PendingScarabs != null && scarabChoicePanel != null)
            {
                scarabChoicePanel.Bind(input);
                scarabChoicePanel.gameObject.SetActive(true);
                _panelOpen = true;
                return;
            }

            if (input.PendingPlayers != null && playerChoicePanel != null)
            {
                playerChoicePanel.Bind(input);
                playerChoicePanel.gameObject.SetActive(true);
                _panelOpen = true;
                return;
            }

            if (input.PendingTileList != null && tileChoicePanel != null)
            {
                tileChoicePanel.Bind(input);
                tileChoicePanel.gameObject.SetActive(true);
                _panelOpen = true;
                return;
            }

            if (input.PendingDiceList != null && dieChoicePanel != null)
            {
                dieChoicePanel.Bind(input);
                dieChoicePanel.gameObject.SetActive(true);
                _panelOpen = true;
                return;
            }

            if (input.PendingDie != null && dieChoicePanel != null)
            {
                dieChoicePanel.BindPipChoice(input);
                dieChoicePanel.gameObject.SetActive(true);
                _panelOpen = true;
                return;
            }

            // Ability or generic yes/no (continue-rolling handled by DiceCupView)
            if (input.PendingAbility != null && yesNoPanel != null)
            {
                yesNoPanel.Bind(input);
                yesNoPanel.gameObject.SetActive(true);
                _panelOpen = true;
                return;
            }

            if (input.PendingPrompt != null && input.PendingAbility == null
                && input.PendingDiceList == null && input.PendingTileList == null
                && input.PendingPlayers == null && input.PendingDie == null
                && yesNoPanel != null)
            {
                yesNoPanel.Bind(input);
                yesNoPanel.gameObject.SetActive(true);
                _panelOpen = true;
            }
        }

        private void CloseOverlayPanels()
        {
            _panelOpen = false;
            SetActive(tileChoicePanel,   false);
            SetActive(dieChoicePanel,    false);
            SetActive(yesNoPanel,        false);
            SetActive(scarabChoicePanel, false);
            SetActive(playerChoicePanel, false);
        }

        private static void SetActive(MonoBehaviour panel, bool active)
        {
            if (panel != null && panel.gameObject.activeSelf != active)
                panel.gameObject.SetActive(active);
        }

        /// <summary>
        /// Called by sub-panels (or binding code) when a decision has been resolved
        /// so this coordinator knows the panel has closed.
        /// </summary>
        public void NotifyPanelClosed()
        {
            _panelOpen = false;
        }
    }
}
