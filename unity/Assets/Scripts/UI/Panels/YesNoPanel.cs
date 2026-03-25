using System;
using UnityEngine;
using FotP.Engine.Players;

namespace FotP.Unity.UI
{
    /// <summary>
    /// Generic yes/no (confirm/cancel) dialog panel.
    /// </summary>
    public class YesNoPanel : MonoBehaviour
    {
        [SerializeField] private GameObject panelRoot;
        [SerializeField] private UnityEngine.UI.Text promptText;
        [SerializeField] private UnityEngine.UI.Button confirmButton;
        [SerializeField] private UnityEngine.UI.Button cancelButton;
        [SerializeField] private UnityEngine.UI.Text confirmLabel;
        [SerializeField] private UnityEngine.UI.Text cancelLabel;

        private Action<bool>? _onResult;

        public void Show(string prompt, Player player,
            string confirmLabel = "Yes", string cancelLabel = "No",
            Action<bool>? onResult = null)
        {
            _onResult = onResult;

            if (promptText != null) promptText.text = prompt;
            if (this.confirmLabel != null) this.confirmLabel.text = confirmLabel;
            if (this.cancelLabel != null) this.cancelLabel.text = cancelLabel;

            confirmButton.onClick.RemoveAllListeners();
            cancelButton.onClick.RemoveAllListeners();
            confirmButton.onClick.AddListener(() => Resolve(true));
            cancelButton.onClick.AddListener(() => Resolve(false));

            panelRoot.SetActive(true);
        }

        private void Resolve(bool result)
        {
            panelRoot.SetActive(false);
            _onResult?.Invoke(result);
        }
    }
}
