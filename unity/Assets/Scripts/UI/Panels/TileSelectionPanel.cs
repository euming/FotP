using System;
using System.Collections.Generic;
using UnityEngine;
using FotP.Engine.Players;
using FotP.Engine.Tiles;

namespace FotP.Unity.UI
{
    /// <summary>
    /// Panel for selecting a tile from a list. Attach to the TileSelection UI prefab.
    /// </summary>
    public class TileSelectionPanel : MonoBehaviour
    {
        [SerializeField] private GameObject panelRoot;
        [SerializeField] private UnityEngine.UI.Button passButton;
        [SerializeField] private UnityEngine.UI.Text headerText;

        private Action<Tile?>? _onResult;

        public void Show(IReadOnlyList<Tile> tiles, Player player,
            bool allowSkip,
            Action<Tile?> onResult,
            string? headerText = null)
        {
            _onResult = onResult;

            if (headerText != null && this.headerText != null)
                this.headerText.text = headerText;

            passButton.gameObject.SetActive(allowSkip);
            passButton.onClick.RemoveAllListeners();
            passButton.onClick.AddListener(() => Resolve(null));

            panelRoot.SetActive(true);
            // TODO: instantiate tile button prefabs for each Tile in tiles
        }

        /// <summary>Called by tile button click.</summary>
        public void SelectTile(Tile tile) => Resolve(tile);

        private void Resolve(Tile? tile)
        {
            panelRoot.SetActive(false);
            _onResult?.Invoke(tile);
        }
    }
}
