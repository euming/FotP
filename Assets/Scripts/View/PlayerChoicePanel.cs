using System;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Players;

namespace FotP.View
{
    /// <summary>
    /// Panel for <c>ChoosePlayer</c> → <c>ResolvePlayer</c>.
    ///
    /// Presents one button per candidate player.  Pressing resolves the decision.
    ///
    /// Inspector setup:
    ///   - <see cref="playerButtonPrefab"/>: Button prefab with a child Text.
    ///     Should have a <see cref="PlayerButtonView"/> component.
    ///   - <see cref="playersContainer"/>: layout group.
    ///   - <see cref="promptLabel"/>: optional header label.
    /// </summary>
    public class PlayerChoicePanel : MonoBehaviour
    {
        [Header("UI References")]
        public Transform        playersContainer;
        public PlayerButtonView playerButtonPrefab;
        public Text             promptLabel;

        private UnityPlayerInput              _input;
        private readonly List<PlayerButtonView> _buttons = new();

        // ── Public API ────────────────────────────────────────────────────────

        public void Bind(UnityPlayerInput input)
        {
            _input = input;

            if (promptLabel != null)
                promptLabel.text = input.PendingPrompt ?? "Choose a player";

            Rebuild(input.PendingPlayers);
        }

        // ── Private ───────────────────────────────────────────────────────────

        private void Rebuild(IReadOnlyList<Player> players)
        {
            foreach (var b in _buttons)
                if (b != null) Destroy(b.gameObject);
            _buttons.Clear();

            if (players == null || playersContainer == null || playerButtonPrefab == null) return;

            foreach (var player in players)
            {
                var go  = Instantiate(playerButtonPrefab.gameObject, playersContainer);
                var pbv = go.GetComponent<PlayerButtonView>();
                pbv.Bind(player, OnPlayerSelected);
                _buttons.Add(pbv);
            }
        }

        private void OnPlayerSelected(Player player)
        {
            if (_input == null) return;
            var inp = _input;
            _input = null;
            gameObject.SetActive(false);
            inp.ResolvePlayer(player);
        }
    }

    // ── PlayerButtonView (inline helper) ─────────────────────────────────────

    /// <summary>
    /// Button that represents a single <see cref="Player"/> in a choice list.
    /// </summary>
    [RequireComponent(typeof(Button))]
    public class PlayerButtonView : MonoBehaviour
    {
        public Text nameLabel;

        public Player Player { get; private set; }
        private Action<Player> _onSelected;
        private Button         _button;

        private void Awake()
        {
            _button = GetComponent<Button>();
            _button.onClick.AddListener(OnClick);
        }

        private void OnDestroy() => _button.onClick.RemoveListener(OnClick);

        public void Bind(Player player, Action<Player> onSelected)
        {
            Player      = player;
            _onSelected = onSelected;
            if (nameLabel != null) nameLabel.text = player?.Name ?? "Player";
        }

        private void OnClick() => _onSelected?.Invoke(Player);
    }
}
