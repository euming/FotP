using System;
using UnityEngine;
using FotP.Engine.Players;

namespace FotP.Unity.UI
{
    /// <summary>
    /// Panel with Roll Again and Stop buttons shown between rolls.
    /// </summary>
    public class ContinueRollingPanel : MonoBehaviour
    {
        [SerializeField] private GameObject panelRoot;
        [SerializeField] private UnityEngine.UI.Button rollAgainButton;
        [SerializeField] private UnityEngine.UI.Button stopButton;
        [SerializeField] private UnityEngine.UI.Text playerNameText;

        private Action<bool>? _onResult;

        public void Show(Player player, Action<bool> onResult)
        {
            _onResult = onResult;

            if (playerNameText != null) playerNameText.text = player.Name;

            rollAgainButton.onClick.RemoveAllListeners();
            stopButton.onClick.RemoveAllListeners();
            rollAgainButton.onClick.AddListener(() => Resolve(true));
            stopButton.onClick.AddListener(() => Resolve(false));

            panelRoot.SetActive(true);
        }

        private void Resolve(bool continueRolling)
        {
            panelRoot.SetActive(false);
            _onResult?.Invoke(continueRolling);
        }
    }
}
