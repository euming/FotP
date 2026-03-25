using System;
using System.Collections.Generic;
using UnityEngine;
using FotP.Engine.Players;

namespace FotP.Unity.UI
{
    /// <summary>
    /// Panel showing player portraits for targeting abilities.
    /// </summary>
    public class PlayerSelectionPanel : MonoBehaviour
    {
        [SerializeField] private GameObject panelRoot;
        [SerializeField] private UnityEngine.UI.Text promptText;
        [SerializeField] private UnityEngine.UI.Button skipButton;

        private Action<Player?>? _onResult;

        public void Show(IReadOnlyList<Player> players, string prompt,
            Player activePlayer, Action<Player?> onResult)
        {
            _onResult = onResult;

            if (promptText != null) promptText.text = prompt;

            skipButton.onClick.RemoveAllListeners();
            skipButton.onClick.AddListener(() => Resolve(null));

            panelRoot.SetActive(true);
            // TODO: instantiate player portrait buttons for each Player in players
        }

        /// <summary>Called by portrait button click.</summary>
        public void SelectPlayer(Player player) => Resolve(player);

        private void Resolve(Player? player)
        {
            panelRoot.SetActive(false);
            _onResult?.Invoke(player);
        }
    }
}
