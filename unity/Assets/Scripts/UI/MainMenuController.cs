using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;
using TMPro;
// Note: Attach this component to the MainMenuCanvas GameObject in MainMenu.unity.
// In the Inspector, assign:
//   Play Button        → PlayButton
//   Quit Button        → QuitButton
//   Player Count Btns  → PlayerCount2Button, PlayerCount3Button, PlayerCount4Button
//   Player Count Label → PlayerCountLabel (TextMeshProUGUI)

namespace FotP.Unity.UI
{
    /// <summary>
    /// Drives the Main Menu scene. Attach to a persistent Canvas GameObject.
    /// Wires up Play, Quit, and player-count buttons; passes the selected count
    /// to GameController before loading the Game scene.
    /// </summary>
    public class MainMenuController : MonoBehaviour
    {
        [Header("UI References")]
        [SerializeField] private Button playButton;
        [SerializeField] private Button quitButton;
        [SerializeField] private Button[] playerCountButtons; // 2, 3, 4 players
        [SerializeField] private TextMeshProUGUI playerCountLabel;

        [Header("Scene Names")]
        [SerializeField] private string gameSceneName = "Game";

        private int _selectedPlayerCount = 2;

        void Start()
        {
            // Wire player-count buttons (expects 3 buttons for 2, 3, 4 players)
            for (int i = 0; i < playerCountButtons.Length; i++)
            {
                int count = i + 2; // 2, 3, 4
                playerCountButtons[i].onClick.AddListener(() => SetPlayerCount(count));
            }

            playButton.onClick.AddListener(OnPlayClicked);
            quitButton.onClick.AddListener(OnQuitClicked);

            UpdatePlayerCountLabel();
        }

        private void SetPlayerCount(int count)
        {
            _selectedPlayerCount = count;
            UpdatePlayerCountLabel();
        }

        private void UpdatePlayerCountLabel()
        {
            if (playerCountLabel != null)
                playerCountLabel.text = $"Players: {_selectedPlayerCount}";
        }

        private void OnPlayClicked()
        {
            PlayerPrefs.SetInt("SelectedPlayerCount", _selectedPlayerCount);
            PlayerPrefs.Save();
            SceneManager.LoadScene(gameSceneName);
        }

        private void OnQuitClicked()
        {
#if UNITY_EDITOR
            UnityEditor.EditorApplication.isPlaying = false;
#else
            Application.Quit();
#endif
        }
    }
}
