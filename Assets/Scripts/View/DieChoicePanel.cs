using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Dice;

namespace FotP.View
{
    /// <summary>
    /// Panel for die-selection decisions:
    ///   - ChooseDie          → ResolveDie         (single die from a list)
    ///   - ChooseMultipleDice → ResolveMultipleDice (toggle-select many dice)
    ///   - ChoosePipValue     → ResolvePipValue     (pick a pip 1-6 for a specific die)
    ///
    /// Inspector setup:
    ///   - <see cref="dieButtonPrefab"/>: prefab with a <see cref="DieButtonView"/>.
    ///   - <see cref="diceContainer"/>: layout group.
    ///   - <see cref="promptLabel"/>: displays the engine's prompt string.
    ///   - <see cref="confirmButton"/>: only shown for multi-die mode.
    ///   - <see cref="pipButtons"/>: six pip-value buttons (values 1-6), shown only
    ///     in pip-choice mode.  Index 0 = pip 1, index 5 = pip 6.
    /// </summary>
    public class DieChoicePanel : MonoBehaviour
    {
        [Header("UI References")]
        public Transform     diceContainer;
        public DieButtonView dieButtonPrefab;
        public Text          promptLabel;
        public Button        confirmButton;

        [Tooltip("Six buttons for pip values 1-6. Shown only in pip-choice mode.")]
        public List<Button> pipButtons = new();

        private UnityPlayerInput          _input;
        private readonly List<DieButtonView> _buttons  = new();
        private readonly List<Die>           _selected = new();

        private enum Mode { Single, Multi, Pip }
        private Mode _mode;

        // ── Lifecycle ─────────────────────────────────────────────────────────

        private void OnEnable()
        {
            if (confirmButton != null)
            {
                confirmButton.onClick.RemoveAllListeners();
                confirmButton.onClick.AddListener(OnConfirmMulti);
            }

            for (int i = 0; i < pipButtons.Count; i++)
            {
                int pip = i + 1; // capture for closure
                if (pipButtons[i] != null)
                {
                    pipButtons[i].onClick.RemoveAllListeners();
                    pipButtons[i].onClick.AddListener(() => OnPipSelected(pip));
                }
            }
        }

        // ── Public API ────────────────────────────────────────────────────────

        /// <summary>Bind for ChooseDie or ChooseMultipleDice.</summary>
        public void Bind(UnityPlayerInput input)
        {
            _input = input;
            _selected.Clear();

            // Determine single vs multi by checking if a multi-TCS is pending.
            // We use a naming convention: PendingDiceList is set for both.
            // We rely on the fact that ChooseMultipleDice always supplies a prompt.
            _mode = Mode.Multi; // safe default; single-die resolves on first click

            if (promptLabel != null)
                promptLabel.text = input.PendingPrompt ?? $"{input.PendingPlayer?.Name}: choose dice";

            SetPipButtonsVisible(false);
            if (confirmButton != null)
                confirmButton.gameObject.SetActive(_mode == Mode.Multi);

            RebuildDiceButtons(input.PendingDiceList, multiSelect: true);
        }

        /// <summary>Bind specifically for ChoosePipValue (single die, set face).</summary>
        public void BindPipChoice(UnityPlayerInput input)
        {
            _input = input;
            _mode  = Mode.Pip;

            if (promptLabel != null)
                promptLabel.text = input.PendingPrompt ?? "Choose a pip value";

            // Clear die buttons – not needed for pip mode
            foreach (var b in _buttons)
                if (b != null) Destroy(b.gameObject);
            _buttons.Clear();

            SetPipButtonsVisible(true);
            if (confirmButton != null)
                confirmButton.gameObject.SetActive(false);
        }

        // ── Private ───────────────────────────────────────────────────────────

        private void RebuildDiceButtons(IReadOnlyList<Die> dice, bool multiSelect)
        {
            foreach (var b in _buttons)
                if (b != null) Destroy(b.gameObject);
            _buttons.Clear();

            if (dice == null || diceContainer == null || dieButtonPrefab == null) return;

            foreach (var die in dice)
            {
                var go  = Instantiate(dieButtonPrefab.gameObject, diceContainer);
                var dbv = go.GetComponent<DieButtonView>();

                if (multiSelect)
                    dbv.Bind(die, OnDieToggled);
                else
                    dbv.Bind(die, OnDieSingleSelected);

                _buttons.Add(dbv);
            }
        }

        private void OnDieToggled(Die die, bool selected)
        {
            if (selected) _selected.Add(die);
            else          _selected.Remove(die);
        }

        private void OnDieSingleSelected(Die die, bool _)
        {
            if (_input == null) return;
            var inp = _input;
            _input = null;
            _selected.Clear();
            gameObject.SetActive(false);
            inp.ResolveDie(die);
        }

        private void OnConfirmMulti()
        {
            if (_input == null) return;
            var inp  = _input;
            var dice = new List<Die>(_selected);
            _input = null;
            _selected.Clear();
            gameObject.SetActive(false);
            inp.ResolveMultipleDice(dice);
        }

        private void OnPipSelected(int pip)
        {
            if (_input == null) return;
            var inp = _input;
            _input = null;
            gameObject.SetActive(false);
            inp.ResolvePipValue(pip);
        }

        private void SetPipButtonsVisible(bool visible)
        {
            foreach (var btn in pipButtons)
                if (btn != null) btn.gameObject.SetActive(visible);
        }
    }
}
