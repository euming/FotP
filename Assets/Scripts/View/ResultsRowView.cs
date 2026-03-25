using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Players;

namespace FotP.View
{
    /// <summary>
    /// A single row in the end-game results leaderboard.
    ///
    /// Attach to a prefab that contains child Text components wired up in the
    /// Inspector.  <see cref="ResultsView"/> instantiates and populates these
    /// rows via <see cref="Populate"/>.
    ///
    /// Typical layout (horizontal):
    ///   [rank]  [player name]  [pyramid score]  [tokens]  [tile count]  [winner crown]
    /// </summary>
    public class ResultsRowView : MonoBehaviour
    {
        [Tooltip("e.g. \"1st\", \"2nd\", \"3rd\"")]
        public Text rankLabel;

        [Tooltip("Player's display name.")]
        public Text playerNameLabel;

        [Tooltip("Sum of locked die pip values (PyramidScore).")]
        public Text pyramidScoreLabel;

        [Tooltip("Remaining token count.")]
        public Text tokensLabel;

        [Tooltip("Number of tiles owned.")]
        public Text tileCountLabel;

        [Tooltip("GameObject shown only for the winner (e.g. a crown icon).")]
        public GameObject winnerIndicator;

        // ──────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Fills the row with data for <paramref name="player"/>.
        /// </summary>
        public void Populate(int rank, Player player, bool isWinner)
        {
            if (rankLabel      != null) rankLabel.text         = OrdinalSuffix(rank);
            if (playerNameLabel != null) playerNameLabel.text   = player.Name;
            if (pyramidScoreLabel != null) pyramidScoreLabel.text = player.PyramidScore.ToString();
            if (tokensLabel    != null) tokensLabel.text       = player.Tokens.ToString();
            if (tileCountLabel != null) tileCountLabel.text    = player.OwnedTiles.Count.ToString();
            if (winnerIndicator != null) winnerIndicator.SetActive(isWinner);
        }

        // ──────────────────────────────────────────────────────────────────────

        private static string OrdinalSuffix(int n)
        {
            if (n <= 0) return n.ToString();
            return (n % 100) switch
            {
                11 or 12 or 13 => $"{n}th",
                _ => (n % 10) switch
                {
                    1 => $"{n}st",
                    2 => $"{n}nd",
                    3 => $"{n}rd",
                    _ => $"{n}th",
                }
            };
        }
    }
}
