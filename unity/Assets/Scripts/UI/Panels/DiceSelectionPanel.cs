using System;
using System.Collections.Generic;
using UnityEngine;
using FotP.Engine.Dice;
using FotP.Engine.Players;

namespace FotP.Unity.UI
{
    /// <summary>
    /// Panel for selecting one or more dice. Attach to the DiceSelection UI prefab.
    /// Wire up die button callbacks and the Done/Skip button in the Inspector.
    /// </summary>
    public class DiceSelectionPanel : MonoBehaviour
    {
        [SerializeField] private GameObject panelRoot;
        [SerializeField] private UnityEngine.UI.Button doneButton;
        [SerializeField] private UnityEngine.UI.Text headerText;

        private IReadOnlyList<Die> _candidates;
        private readonly HashSet<Die> _selected = new HashSet<Die>();
        private int _minSelect;
        private Action<List<Die>>? _onConfirm;
        private Action<Die?>? _onSingleResult;
        private bool _allowSkipSingle;

        /// <summary>Show multi-select panel. minSelect=0 allows empty selection.</summary>
        public void Show(IReadOnlyList<Die> dice, Player player,
            int minSelect, bool multiSelect,
            string? headerText = null,
            Action<List<Die>>? onConfirm = null)
        {
            _candidates = dice;
            _selected.Clear();
            _minSelect = minSelect;
            _onConfirm = onConfirm;

            if (headerText != null && this.headerText != null)
                this.headerText.text = headerText;

            doneButton.onClick.RemoveAllListeners();
            doneButton.onClick.AddListener(ConfirmSelection);

            panelRoot.SetActive(true);
            // TODO: instantiate die button prefabs for each Die in dice
        }

        /// <summary>Show single-select panel with optional skip.</summary>
        public void ShowSingle(IReadOnlyList<Die> dice, Player player, string prompt,
            bool allowSkip,
            Action<Die?> onResult)
        {
            _candidates = dice;
            _selected.Clear();
            _minSelect = allowSkip ? 0 : 1;
            _allowSkipSingle = allowSkip;
            _onSingleResult = onResult;

            if (headerText != null) headerText.text = prompt;

            doneButton.onClick.RemoveAllListeners();
            doneButton.onClick.AddListener(ConfirmSingle);

            panelRoot.SetActive(true);
            // TODO: instantiate die button prefabs for each Die in dice
        }

        /// <summary>Called by die button click. Toggles die in selection set.</summary>
        public void ToggleDie(Die die)
        {
            if (_selected.Contains(die)) _selected.Remove(die);
            else _selected.Add(die);
            doneButton.interactable = _selected.Count >= _minSelect;
        }

        private void ConfirmSelection()
        {
            panelRoot.SetActive(false);
            _onConfirm?.Invoke(new List<Die>(_selected));
        }

        private void ConfirmSingle()
        {
            panelRoot.SetActive(false);
            _onSingleResult?.Invoke(_selected.Count > 0
                ? System.Linq.Enumerable.First(_selected)
                : null);
        }
    }
}
