using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Tiles;

namespace FotP.View
{
    /// <summary>
    /// Displays a single owned tile on the player's board.
    /// Shows tile name, level, color, and whether it is an artifact.
    /// </summary>
    public class OwnedTileView : MonoBehaviour
    {
        [Header("UI References")]
        public Text tileNameLabel;
        public Text levelLabel;
        public Image colorBadge;
        public GameObject artifactIcon;

        [Header("Tile Color Tints")]
        public Color yellowTint = new Color(1f, 0.9f, 0.2f);
        public Color blueTint   = new Color(0.3f, 0.6f, 1f);
        public Color redTint    = new Color(1f, 0.3f, 0.3f);

        public void Bind(Tile tile)
        {
            if (tileNameLabel != null)
                tileNameLabel.text = tile.Name;

            if (levelLabel != null)
                levelLabel.text = $"L{tile.Level}";

            if (colorBadge != null)
                colorBadge.color = TileColorToUnity(tile.Color);

            if (artifactIcon != null)
                artifactIcon.SetActive(tile.IsArtifact);
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
