using System;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Players;

namespace FotP.View
{
    /// <summary>
    /// Panel for <c>ChooseScarab</c> → <c>ResolveScarab</c>.
    ///
    /// Presents one button per available scarab.  Pressing resolves the decision.
    ///
    /// Inspector setup:
    ///   - <see cref="scarabButtonPrefab"/>: Button prefab with a Text child.
    ///     Should have a <see cref="ScarabButtonView"/> component.
    ///   - <see cref="scarabsContainer"/>: layout group.
    ///   - <see cref="promptLabel"/>: optional header.
    /// </summary>
    public class ScarabChoicePanel : MonoBehaviour
    {
        [Header("UI References")]
        public Transform        scarabsContainer;
        public ScarabButtonView scarabButtonPrefab;
        public Text             promptLabel;

        private UnityPlayerInput             _input;
        private readonly List<ScarabButtonView> _buttons = new();

        // ── Public API ────────────────────────────────────────────────────────

        public void Bind(UnityPlayerInput input)
        {
            _input = input;

            if (promptLabel != null)
                promptLabel.text = $"{input.PendingPlayer?.Name}: choose a scarab";

            Rebuild(input.PendingScarabs);
        }

        // ── Private ───────────────────────────────────────────────────────────

        private void Rebuild(IReadOnlyList<Scarab> scarabs)
        {
            foreach (var b in _buttons)
                if (b != null) Destroy(b.gameObject);
            _buttons.Clear();

            if (scarabs == null || scarabsContainer == null || scarabButtonPrefab == null) return;

            foreach (var scarab in scarabs)
            {
                var go  = Instantiate(scarabButtonPrefab.gameObject, scarabsContainer);
                var sbv = go.GetComponent<ScarabButtonView>();
                sbv.Bind(scarab, OnScarabSelected);
                _buttons.Add(sbv);
            }
        }

        private void OnScarabSelected(Scarab scarab)
        {
            if (_input == null) return;
            var inp = _input;
            _input = null;
            gameObject.SetActive(false);
            inp.ResolveScarab(scarab);
        }
    }

    // ── ScarabButtonView (inline helper) ──────────────────────────────────────

    /// <summary>
    /// Button that represents a single <see cref="Scarab"/> in a choice list.
    /// </summary>
    [RequireComponent(typeof(Button))]
    public class ScarabButtonView : MonoBehaviour
    {
        public Text nameLabel;

        public Scarab Scarab { get; private set; }
        private Action<Scarab> _onSelected;
        private Button         _button;

        private void Awake()
        {
            _button = GetComponent<Button>();
            _button.onClick.AddListener(OnClick);
        }

        private void OnDestroy() => _button.onClick.RemoveListener(OnClick);

        public void Bind(Scarab scarab, Action<Scarab> onSelected)
        {
            Scarab      = scarab;
            _onSelected = onSelected;
            if (nameLabel != null) nameLabel.text = scarab?.EntityName ?? scarab?.Type.ToString() ?? "Scarab";
        }

        private void OnClick() => _onSelected?.Invoke(Scarab);
    }
}
