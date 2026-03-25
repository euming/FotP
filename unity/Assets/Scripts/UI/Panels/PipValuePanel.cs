using System;
using UnityEngine;
using FotP.Engine.Dice;
using FotP.Engine.Players;

namespace FotP.Unity.UI
{
    /// <summary>
    /// Number-picker popup letting the player choose a pip value 1–6.
    /// Wire six buttons (labeled 1–6) to SelectValue(int).
    /// </summary>
    public class PipValuePanel : MonoBehaviour
    {
        [SerializeField] private GameObject panelRoot;
        [SerializeField] private UnityEngine.UI.Text promptText;

        private Action<int>? _onResult;

        public void Show(Die die, string prompt, Player player, Action<int> onResult)
        {
            _onResult = onResult;
            if (promptText != null) promptText.text = prompt;
            panelRoot.SetActive(true);
            // Six pip-value buttons (1–6) each call SelectValue(n) via UnityEvent.
        }

        /// <summary>Called by pip-value button click (1–6).</summary>
        public void SelectValue(int value)
        {
            if (value < 1 || value > 6) return;
            panelRoot.SetActive(false);
            _onResult?.Invoke(value);
        }
    }
}
