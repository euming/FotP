using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Tiles;

namespace FotP.View
{
    /// <summary>
    /// Panel for tile-selection decisions:
    ///   - ChooseTileToClaim  → ResolveClaimTile
    ///   - ChooseTile         → ResolveTile
    ///
    /// Presents one button per candidate tile.  Pressing a button resolves the
    /// pending decision and hides the panel.
    ///
    /// Inspector setup:
    ///   - <see cref="tileButtonPrefab"/>: Button prefab with a child Text showing
    ///     the tile name. Should have a <see cref="TileButtonView"/> component.
    ///   - <see cref="tilesContainer"/>: layout group that holds instantiated buttons.
    ///   - <see cref="promptLabel"/>: optional header label.
    /// </summary>
    public class TileChoicePanel : MonoBehaviour
    {
        [Header("UI References")]
        public Transform       tilesContainer;
        public TileButtonView  tileButtonPrefab;
        public Text            promptLabel;

        private UnityPlayerInput          _input;
        private bool                       _isClaim;
        private readonly List<TileButtonView> _buttons = new();

        // ── Public API ────────────────────────────────────────────────────────

        /// <summary>Bind to an input awaiting a tile selection.</summary>
        public void Bind(UnityPlayerInput input)
        {
            _input   = input;
            _isClaim = input.PendingPrompt == null; // ChooseTileToClaim has no prompt

            string header = _isClaim
                ? $"{input.PendingPlayer?.Name}: choose a tile to claim"
                : (input.PendingPrompt ?? "Choose a tile");

            if (promptLabel != null)
                promptLabel.text = header;

            Rebuild(input.PendingTileList);
        }

        // ── Private ───────────────────────────────────────────────────────────

        private void Rebuild(IReadOnlyList<Tile> tiles)
        {
            foreach (var b in _buttons)
                if (b != null) Destroy(b.gameObject);
            _buttons.Clear();

            if (tiles == null || tilesContainer == null || tileButtonPrefab == null) return;

            foreach (var tile in tiles)
            {
                var go  = Instantiate(tileButtonPrefab.gameObject, tilesContainer);
                var tbv = go.GetComponent<TileButtonView>();
                tbv.Bind(tile, OnTileSelected);
                _buttons.Add(tbv);
            }
        }

        private void OnTileSelected(Tile tile)
        {
            if (_input == null) return;
            var inp    = _input;
            var isClaim = _isClaim;
            _input = null;
            gameObject.SetActive(false);

            if (isClaim)
                inp.ResolveClaimTile(tile);
            else
                inp.ResolveTile(tile);
        }
    }
}
