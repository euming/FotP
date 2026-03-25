using System;
using System.Collections.Generic;
using UnityEngine;
using FotP.Engine.Players;
using FotP.Engine.Tiles;

namespace FotP.Unity.UI
{
    /// <summary>
    /// Panel for choosing a scarab from the player's supply, or passing.
    /// </summary>
    public class ScarabSelectionPanel : MonoBehaviour
    {
        [SerializeField] private GameObject panelRoot;
        [SerializeField] private UnityEngine.UI.Button passButton;

        private Action<Scarab?>? _onResult;

        public void Show(IReadOnlyList<Scarab> scarabs, Player player, Action<Scarab?> onResult)
        {
            _onResult = onResult;

            passButton.onClick.RemoveAllListeners();
            passButton.onClick.AddListener(() => Resolve(null));

            panelRoot.SetActive(true);
            // TODO: instantiate scarab button prefabs for each Scarab in scarabs
        }

        /// <summary>Called by scarab button click.</summary>
        public void SelectScarab(Scarab scarab) => Resolve(scarab);

        private void Resolve(Scarab? scarab)
        {
            panelRoot.SetActive(false);
            _onResult?.Invoke(scarab);
        }
    }
}
