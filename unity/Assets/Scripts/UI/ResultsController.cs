using System.Text;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;
using TMPro;
using FotP.Engine.Players;

// Note: Attach this component to the ResultsCanvas GameObject in Results.unity.
// In the Inspector, assign:
//   Winner Label      → WinnerLabel (TextMeshProUGUI)
//   Scores Label      → ScoresLabel (TextMeshProUGUI)
//   Rematch Button    → RematchButton
//   Main Menu Button  → MainMenuButton

namespace FotP.Unity.UI
{
    /// <summary>
    /// Drives the Results scene. Reads final game state from GameController.Instance,
    /// displays winner, scores, and tiles collected, then offers Rematch or Main Menu.
    /// </summary>
    public class ResultsController : MonoBehaviour
    {
        [Header("UI References")]
        [SerializeField] private TextMeshProUGUI winnerLabel;
        [SerializeField] private TextMeshProUGUI scoresLabel;
        [SerializeField] private Button rematchButton;
        [SerializeField] private Button mainMenuButton;

        [Header("Scene Names")]
        [SerializeField] private string gameSceneName = "Game";
        [SerializeField] private string mainMenuSceneName = "MainMenu";

        void Start()
        {
            PopulateResults();

            rematchButton.onClick.AddListener(OnRematch);
            mainMenuButton.onClick.AddListener(OnMainMenu);
        }

        private void PopulateResults()
        {
            var gc = GameController.Instance;
            if (gc == null || gc.State == null)
            {
                if (winnerLabel != null) winnerLabel.text = "No game data.";
                return;
            }

            var state = gc.State;
            Player winner = state.DetermineWinner();

            if (winnerLabel != null)
                winnerLabel.text = $"{winner.Name} Wins!";

            if (scoresLabel != null)
            {
                var sb = new StringBuilder();
                foreach (var player in state.TurnOrder)
                {
                    sb.AppendLine($"{player.Name}");
                    sb.AppendLine($"  Score: {player.PyramidScore}");
                    sb.AppendLine($"  Tiles: {player.OwnedTiles.Count}");
                    sb.AppendLine();
                }
                scoresLabel.text = sb.ToString().TrimEnd();
            }
        }

        private void OnRematch()
        {
            // Re-use the same player count; GameController.Start() will re-initialise.
            SceneManager.LoadScene(gameSceneName);
        }

        private void OnMainMenu()
        {
            SceneManager.LoadScene(mainMenuSceneName);
        }
    }
}
