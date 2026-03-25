using System;
using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Tiles;

namespace FotP.View
{
    /// <summary>
    /// A button that represents a single <see cref="Tile"/> in a selection list.
    ///
    /// Shows the tile name and level.  Pressing fires the <see cref="OnSelected"/>
    /// callback with the bound tile.
    ///
    /// Inspector setup:
    ///   - <see cref="nameLabel"/>: Text component for the tile name.
    ///   - <see cref="levelLabel"/>: optional Text for the tile level.
    /// </summary>
    [RequireComponent(typeof(Button))]
    public class TileButtonView : MonoBehaviour
    {
        [Header("UI References")]
        public Text nameLabel;
        public Text levelLabel;

        public Tile Tile { get; private set; }

        private Action<Tile> _onSelected;
        private Button       _button;

        private void Awake()
        {
            _button = GetComponent<Button>();
            _button.onClick.AddListener(OnClick);
        }

        private void OnDestroy()
        {
            _button.onClick.RemoveListener(OnClick);
        }

        /// <summary>Attach to a tile and register a selection callback.</summary>
        public void Bind(Tile tile, Action<Tile> onSelected)
        {
            Tile        = tile;
            _onSelected = onSelected;
            Refresh();
        }

        private void Refresh()
        {
            if (nameLabel  != null) nameLabel.text  = Tile?.Name  ?? "?";
            if (levelLabel != null) levelLabel.text = Tile != null ? $"Lv {Tile.Level}" : "";
        }

        private void OnClick() => _onSelected?.Invoke(Tile);
    }
}
