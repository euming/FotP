using UnityEngine;
using UnityEngine.UI;

namespace FotP.View
{
    /// <summary>
    /// Panel for boolean decisions that are not already handled by <see cref="DiceCupView"/>:
    ///   - ChooseYesNo      → ResolveYesNo
    ///   - ChooseUseAbility → ResolveUseAbility
    ///
    /// Inspector setup:
    ///   - <see cref="promptLabel"/>: displays the question text.
    ///   - <see cref="yesButton"/> / <see cref="noButton"/>: answer buttons.
    ///   - <see cref="yesLabel"/> / <see cref="noLabel"/>: optional button text labels.
    /// </summary>
    public class YesNoPanel : MonoBehaviour
    {
        [Header("UI References")]
        public Text   promptLabel;
        public Button yesButton;
        public Button noButton;
        public Text   yesLabel;
        public Text   noLabel;

        private UnityPlayerInput _input;
        private bool             _isAbility;

        // ── Lifecycle ─────────────────────────────────────────────────────────

        private void OnEnable()
        {
            if (yesButton != null)
            {
                yesButton.onClick.RemoveAllListeners();
                yesButton.onClick.AddListener(OnYes);
            }
            if (noButton != null)
            {
                noButton.onClick.RemoveAllListeners();
                noButton.onClick.AddListener(OnNo);
            }
        }

        // ── Public API ────────────────────────────────────────────────────────

        /// <summary>
        /// Bind to an input awaiting a yes/no or ability-use answer.
        /// </summary>
        public void Bind(UnityPlayerInput input)
        {
            _input     = input;
            _isAbility = input.PendingAbility != null;

            if (_isAbility)
            {
                var abilityName = input.PendingAbility!.EntityName
                                  ?? input.PendingAbility.GetType().Name;
                SetLabels($"Use ability: {abilityName}?", "Use", "Skip");
            }
            else
            {
                SetLabels(input.PendingPrompt ?? "Yes or No?", "Yes", "No");
            }
        }

        // ── Private ───────────────────────────────────────────────────────────

        private void SetLabels(string prompt, string yes, string no)
        {
            if (promptLabel != null) promptLabel.text = prompt;
            if (yesLabel    != null) yesLabel.text    = yes;
            if (noLabel     != null) noLabel.text     = no;
        }

        private void OnYes() => Resolve(true);
        private void OnNo()  => Resolve(false);

        private void Resolve(bool answer)
        {
            if (_input == null) return;
            var inp       = _input;
            var isAbility = _isAbility;
            _input = null;
            gameObject.SetActive(false);

            if (isAbility)
                inp.ResolveUseAbility(answer);
            else
                inp.ResolveYesNo(answer);
        }
    }
}
