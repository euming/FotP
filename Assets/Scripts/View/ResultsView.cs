using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;
using FotP.Engine.Players;

namespace FotP.View
{
    /// <summary>
    /// Results screen displayed when the game ends.
    ///
    /// Subscribe to <see cref="GameController.OnGameOver"/> to activate this panel
    /// and populate the leaderboard.
    ///
    /// Setup in the Inspector:
    ///   - Assign <see cref="gameController"/> (or let it auto-find on the same GameObject).
    ///   - Assign <see cref="winnerLabel"/> for the winner banner.
    ///   - Assign <see cref="playerRowPrefab"/> — a prefab with a <see cref="ResultsRowView"/> component.
    ///   - Assign <see cref="rowsContainer"/> — a VerticalLayoutGroup transform that receives the rows.
    ///   - Optionally assign <see cref="playAgainButton"/> and <see cref="mainMenuButton"/>.
    ///
    /// The panel should start disabled; it enables itself when the game ends.
    /// </summary>
    public class ResultsView : MonoBehaviour
    {
        // ── Engine access ──────────────────────────────────────────────────────

        [Header("Engine")]
        [Tooltip("Provides access to GameState and the OnGameOver event.")]
        public GameController gameController;

        // ── Layout references ──────────────────────────────────────────────────

        [Header("UI References")]
        [Tooltip("Big label at the top, e.g. \"Player 1 wins!\"")]
        public Text winnerLabel;

        [Tooltip("Container for per-player score rows (VerticalLayoutGroup recommended).")]
        public Transform rowsContainer;

        [Tooltip("Prefab that has a ResultsRowView component.")]
        public ResultsRowView playerRowPrefab;

        [Header("Buttons")]
        [Tooltip("Reloads the active scene to start a new game. Optional.")]
        public Button playAgainButton;

        [Tooltip("Loads the scene named 'MainMenu'. Optional.")]
        public Button mainMenuButton;

        [Tooltip("Name of the main-menu scene to load. Defaults to \"MainMenu\".")]
        public string mainMenuSceneName = "MainMenu";

        // ──────────────────────────────────────────────────────────────────────

        void Awake()
        {
            // Hide until the game ends.
            gameObject.SetActive(false);
        }

        void Start()
        {
            if (gameController == null)
                gameController = FindObjectOfType<GameController>();

            if (gameController != null)
                gameController.OnGameOver += HandleGameOver;

            if (playAgainButton != null)
                playAgainButton.onClick.AddListener(OnPlayAgain);

            if (mainMenuButton != null)
                mainMenuButton.onClick.AddListener(OnMainMenu);
        }

        void OnDestroy()
        {
            if (gameController != null)
                gameController.OnGameOver -= HandleGameOver;
        }

        // ──────────────────────────────────────────────────────────────────────
        // Event handler
        // ──────────────────────────────────────────────────────────────────────

        private void HandleGameOver(string winnerName)
        {
            gameObject.SetActive(true);

            if (winnerLabel != null)
                winnerLabel.text = $"{winnerName} wins!";

            PopulateRows(winnerName);
        }

        // ──────────────────────────────────────────────────────────────────────
        // Leaderboard population
        // ──────────────────────────────────────────────────────────────────────

        private void PopulateRows(string winnerName)
        {
            if (rowsContainer == null || playerRowPrefab == null) return;

            // Clear existing rows.
            foreach (Transform child in rowsContainer)
                Destroy(child.gameObject);

            var state = gameController?.Engine?.State;
            if (state == null) return;

            // Sort players: winner first, then descending by pyramid score.
            var ordered = state.TurnOrder
                .OrderByDescending(p => p.Name == winnerName ? 1 : 0)
                .ThenByDescending(p => p.PyramidScore)
                .ThenByDescending(p => p.Tokens)
                .ToList();

            for (int i = 0; i < ordered.Count; i++)
            {
                var row = Instantiate(playerRowPrefab, rowsContainer);
                row.Populate(
                    rank:          i + 1,
                    player:        ordered[i],
                    isWinner:      ordered[i].Name == winnerName
                );
            }
        }

        // ──────────────────────────────────────────────────────────────────────
        // Button callbacks
        // ──────────────────────────────────────────────────────────────────────

        private void OnPlayAgain()
        {
            SceneManager.LoadScene(SceneManager.GetActiveScene().name);
        }

        private void OnMainMenu()
        {
            SceneManager.LoadScene(mainMenuSceneName);
        }
    }
}
