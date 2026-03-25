using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Dice;

namespace FotP.View
{
    /// <summary>
    /// Displays a single die: its pip value (or symbol), type label, and selection state.
    /// Reused by ActiveDiceAreaView and LockedPyramidView.
    /// </summary>
    public class DieView : MonoBehaviour
    {
        [Header("UI References")]
        [Tooltip("Label showing pip value, '*' for star, '**' for double-star")]
        public Text valueLabel;

        [Tooltip("Label showing die type abbreviation (e.g. 'STD', 'NOB', 'ART')")]
        public Text typeLabel;

        [Tooltip("Background image; tinted when selected")]
        public Image background;

        [Tooltip("Highlight overlay shown when this die is selected for locking")]
        public GameObject selectedOverlay;

        [Header("Colors")]
        public Color normalColor    = Color.white;
        public Color selectedColor  = new Color(0.4f, 1f, 0.4f);
        public Color lockedColor    = new Color(1f, 0.85f, 0.3f);

        /// <summary>The engine die this view represents.</summary>
        public Die Die { get; private set; }

        /// <summary>True when this die is toggled for locking in the active area.</summary>
        public bool IsSelected { get; private set; }

        // Callback fired when the user clicks this die.
        private System.Action<DieView> _onClick;

        // -----------------------------------------------------------------------
        // Public API
        // -----------------------------------------------------------------------

        /// <summary>Bind to a die and register a click handler.</summary>
        public void Bind(Die die, System.Action<DieView> onClick)
        {
            Die      = die;
            _onClick = onClick;
            IsSelected = false;
            Refresh();
        }

        /// <summary>Re-read die state and update all labels.</summary>
        public void Refresh()
        {
            if (Die == null) return;

            if (valueLabel != null)
            {
                valueLabel.text = Die.IsDoubleStarFace ? "**"
                                : Die.IsStarFace       ? "*"
                                : Die.HasPipValue      ? Die.PipValue.ToString()
                                :                        "?";
            }

            if (typeLabel != null)
                typeLabel.text = AbbreviateType(Die.DieType);

            RefreshVisuals();
        }

        /// <summary>Toggle selection state (used by ActiveDiceAreaView).</summary>
        public void SetSelected(bool selected)
        {
            IsSelected = selected;
            RefreshVisuals();
        }

        /// <summary>Mark this die as locked (used by LockedPyramidView).</summary>
        public void SetLocked()
        {
            IsSelected = false;
            if (background != null) background.color = lockedColor;
            if (selectedOverlay != null) selectedOverlay.SetActive(false);
        }

        // -----------------------------------------------------------------------
        // Unity
        // -----------------------------------------------------------------------

        public void OnPointerClick()
        {
            _onClick?.Invoke(this);
        }

        // Support both Button onClick and legacy OnMouseDown
        public void OnMouseDown() => _onClick?.Invoke(this);

        // -----------------------------------------------------------------------
        // Helpers
        // -----------------------------------------------------------------------

        private void RefreshVisuals()
        {
            if (background != null)
                background.color = IsSelected ? selectedColor : normalColor;

            if (selectedOverlay != null)
                selectedOverlay.SetActive(IsSelected);
        }

        private static string AbbreviateType(DieType t) => t switch
        {
            DieType.Standard  => "STD",
            DieType.Immediate => "IMM",
            DieType.Serf      => "SRF",
            DieType.Noble     => "NOB",
            DieType.Artisan   => "ART",
            DieType.Intrigue  => "INT",
            DieType.Voyage    => "VOY",
            DieType.Decree    => "DEC",
            _                 => "?",
        };
    }
}
