using System;
using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Dice;

namespace FotP.View
{
    /// <summary>
    /// A togglable button that represents a single <see cref="Die"/>.
    ///
    /// Shows the die's current face value as a label.
    /// Pressing the button toggles selection and fires <see cref="OnToggled"/>.
    ///
    /// Inspector setup:
    ///   - <see cref="faceLabel"/>: Text component that displays the pip count.
    ///   - <see cref="selectedColor"/> / <see cref="deselectedColor"/>: tint colours.
    /// </summary>
    [RequireComponent(typeof(Button))]
    public class DieButtonView : MonoBehaviour
    {
        [Header("UI References")]
        public Text  faceLabel;

        [Header("Visual")]
        public Color deselectedColor = Color.white;
        public Color selectedColor   = new Color(0.4f, 0.8f, 0.4f);

        // ── Runtime ───────────────────────────────────────────────────────────

        public Die Die { get; private set; }
        public bool IsSelected { get; private set; }

        private Action<Die, bool> _onToggled;
        private Button            _button;
        private Image             _image;

        // ── Lifecycle ─────────────────────────────────────────────────────────

        private void Awake()
        {
            _button = GetComponent<Button>();
            _image  = GetComponent<Image>();
            _button.onClick.AddListener(Toggle);
        }

        private void OnDestroy()
        {
            _button.onClick.RemoveListener(Toggle);
        }

        // ── Public API ────────────────────────────────────────────────────────

        /// <summary>Attach to a die and register a toggle callback.</summary>
        public void Bind(Die die, Action<Die, bool> onToggled)
        {
            Die        = die;
            _onToggled = onToggled;
            IsSelected = false;
            Refresh();
        }

        /// <summary>Force-set selection state without firing the callback.</summary>
        public void SetSelected(bool selected)
        {
            IsSelected = selected;
            Refresh();
        }

        // ── Private ───────────────────────────────────────────────────────────

        private void Toggle()
        {
            IsSelected = !IsSelected;
            Refresh();
            _onToggled?.Invoke(Die, IsSelected);
        }

        private void Refresh()
        {
            if (faceLabel != null)
            {
                if (Die == null)            faceLabel.text = "?";
                else if (Die.IsStarFace)    faceLabel.text = Die.IsDoubleStarFace ? "★★" : "★";
                else                        faceLabel.text = Die.PipValue.ToString();
            }

            if (_image != null)
                _image.color = IsSelected ? selectedColor : deselectedColor;
        }
    }
}
