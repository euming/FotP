using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Players;

namespace FotP.View
{
    /// <summary>
    /// Root view for the game board.
    /// Owns the market display (one <see cref="MarketBarView"/> per level) and
    /// the current player's tile display.
    ///
    /// Usage:
    ///   1. Assign <see cref="gameController"/> in the inspector (or let it auto-find).
    ///   2. Call <see cref="Refresh"/> after any engine state change to redraw everything.
    ///
    /// The view is read-only with respect to the engine; it does not mutate game state.
    /// </summary>
    public class BoardView : MonoBehaviour
    {
        // ── Engine access ──────────────────────────────────────────────────────

        [Header("Engine")]
        [Tooltip("Provides access to FotP.Engine.State.GameState. " +
                 "Usually auto-found via GetComponent on the same GameObject.")]
        public GameController gameController;

        // ── Market section ─────────────────────────────────────────────────────

        [Header("Market – Bars")]
        [Tooltip("Container that holds one MarketBarView child per level. " +
                 "Should have a VerticalLayoutGroup component.")]
        public Transform marketBarsContainer;

        [Tooltip("Prefab for a single level bar (must have a MarketBarView component).")]
        public MarketBarView marketBarPrefab;

        // ── Player tile display ────────────────────────────────────────────────

        [Header("Player Tile Display")]
        [Tooltip("Container for the active player's owned tiles. " +
                 "Should have a GridLayoutGroup or HorizontalLayoutGroup.")]
        public Transform ownedTilesContainer;

        [Tooltip("Prefab for a single owned tile card (must have an OwnedTileView component).")]
        public OwnedTileView ownedTilePrefab;

        [Tooltip("Label that shows the current player's name.")]
        public Text playerNameLabel;

        [Tooltip("Label showing the current player's pyramid score.")]
        public Text pyramidScoreLabel;

        // ── Internal state ─────────────────────────────────────────────────────

        private readonly Dictionary<int, MarketBarView> _barViewsByLevel = new();

        // ── Unity lifecycle ────────────────────────────────────────────────────

        private void Awake()
        {
            if (gameController == null)
                gameController = GetComponent<GameController>();
        }

        private void Start()
        {
            if (gameController == null)
            {
                Debug.LogWarning("[BoardView] No GameController found. " +
                                 "Assign one in the inspector or place on the same GameObject.");
                return;
            }

            BuildMarketBars();
            RefreshPlayerDisplay();
        }

        // ── Public API ─────────────────────────────────────────────────────────

        /// <summary>
        /// Full redraw: rebuilds bars if the market changed structure,
        /// then refreshes all slot counts and the player tile panel.
        /// Safe to call every frame or after each engine action.
        /// </summary>
        public void Refresh()
        {
            if (gameController?.Engine?.State == null) return;

            RefreshMarketBars();
            RefreshPlayerDisplay();
        }

        // ── Market helpers ─────────────────────────────────────────────────────

        /// <summary>
        /// Instantiate one <see cref="MarketBarView"/> per distinct level in the market
        /// and build its slots. Called once at Start (or when a new game begins).
        /// </summary>
        public void BuildMarketBars()
        {
            ClearBarViews();

            if (gameController?.Engine?.State == null) return;
            if (marketBarsContainer == null || marketBarPrefab == null) return;

            var stacks = gameController.Engine.State.Market.Stacks;
            var levels = stacks.Select(s => s.Prototype.Level).Distinct().OrderBy(l => l);

            foreach (int lvl in levels)
            {
                var barGo = Instantiate(marketBarPrefab.gameObject, marketBarsContainer);
                var barView = barGo.GetComponent<MarketBarView>();
                barView.level = lvl;
                barView.Build(stacks.Where(s => s.Prototype.Level == lvl));
                _barViewsByLevel[lvl] = barView;
            }
        }

        /// <summary>
        /// Refresh only the slot labels (counts, exhausted dimming) without
        /// recreating the bar GameObjects. Cheap – call after any tile claim.
        /// </summary>
        public void RefreshMarketBars()
        {
            foreach (var barView in _barViewsByLevel.Values)
                barView.Refresh();
        }

        private void ClearBarViews()
        {
            foreach (var barView in _barViewsByLevel.Values)
            {
                if (barView != null)
                    Destroy(barView.gameObject);
            }
            _barViewsByLevel.Clear();
        }

        // ── Player tile display helpers ────────────────────────────────────────

        /// <summary>
        /// Rebuild the owned-tile panel for the current player.
        /// Called on Start and after the current player changes.
        /// </summary>
        public void RefreshPlayerDisplay()
        {
            if (ownedTilesContainer == null || ownedTilePrefab == null) return;

            // Clear existing tile cards
            foreach (Transform child in ownedTilesContainer)
                Destroy(child.gameObject);

            Player? player = gameController?.Engine?.State?.CurrentPlayer;
            if (player == null) return;

            if (playerNameLabel != null)
                playerNameLabel.text = player.Name;

            if (pyramidScoreLabel != null)
                pyramidScoreLabel.text = $"Score: {player.PyramidScore}";

            foreach (var tile in player.OwnedTiles)
            {
                var tileGo = Instantiate(ownedTilePrefab.gameObject, ownedTilesContainer);
                var tileView = tileGo.GetComponent<OwnedTileView>();
                tileView?.Bind(tile);
            }
        }
    }
}
