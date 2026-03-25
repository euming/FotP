using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Market;
using FotP.Engine.Tiles;

namespace FotP.View
{
    /// <summary>
    /// Displays a single market slot (one tile type + remaining count) in the purchase bar.
    /// Attach to a prefab that has child UI Text components for name, count, and criteria.
    /// </summary>
    public class MarketSlotView : MonoBehaviour
    {
        [Header("UI References")]
        [Tooltip("Displays the tile name (e.g. 'Vizier', 'Estate')")]
        public Text tileNameLabel;

        [Tooltip("Displays remaining stack count (e.g. '3x')")]
        public Text countLabel;

        [Tooltip("Displays the claim criteria (e.g. '2Y+1R')")]
        public Text criteriaLabel;

        [Tooltip("Background image tinted by tile color")]
        public Image colorBadge;

        [Tooltip("Dimmed when stack is exhausted")]
        public CanvasGroup canvasGroup;

        // Colors used to tint the badge by TileColor
        [Header("Tile Color Tints")]
        public Color yellowTint = new Color(1f, 0.9f, 0.2f);
        public Color blueTint   = new Color(0.3f, 0.6f, 1f);
        public Color redTint    = new Color(1f, 0.3f, 0.3f);

        private TileStack _stack;

        /// <summary>
        /// Bind this view to a TileStack and update all labels.
        /// </summary>
        public void Bind(TileStack stack)
        {
            _stack = stack;
            Refresh();
        }

        /// <summary>
        /// Re-read current stack state and update labels.
        /// Call after any market mutation (tile claimed, etc.).
        /// </summary>
        public void Refresh()
        {
            if (_stack == null) return;

            var tile = _stack.Prototype;

            if (tileNameLabel != null)
                tileNameLabel.text = tile.Name;

            if (countLabel != null)
                countLabel.text = _stack.IsEmpty ? "—" : $"{_stack.Remaining}x";

            if (criteriaLabel != null)
                criteriaLabel.text = tile.ClaimCriteria?.Description ?? "";

            if (colorBadge != null)
                colorBadge.color = TileColorToUnity(tile.Color);

            // Dim exhausted slots
            if (canvasGroup != null)
                canvasGroup.alpha = _stack.IsEmpty ? 0.35f : 1f;
        }

        private Color TileColorToUnity(TileColor color)
        {
            return color switch
            {
                TileColor.Yellow => yellowTint,
                TileColor.Blue   => blueTint,
                TileColor.Red    => redTint,
                _                => Color.white,
            };
        }
    }
}
