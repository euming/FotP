using System.Collections.Generic;
using System.Threading.Tasks;
using UnityEngine;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.Tiles;

namespace FotP.Unity.UI
{
    /// <summary>
    /// Unity implementation of IPlayerInput. Each method blocks the game-logic thread
    /// (which runs on a background Task) while the main thread shows UI and awaits
    /// the player's choice via TaskCompletionSource.
    ///
    /// Usage: Attach to a persistent GameObject in the scene. Assign UI panel
    /// references in the Inspector. The GameRunner passes this as each human
    /// player's IPlayerInput.
    /// </summary>
    public class UnityPlayerInput : MonoBehaviour, IPlayerInput
    {
        // ── Inspector references ──────────────────────────────────────────────
        [Header("Panels")]
        [SerializeField] private DiceSelectionPanel diceSelectionPanel;
        [SerializeField] private TileSelectionPanel tileSelectionPanel;
        [SerializeField] private YesNoPanel yesNoPanel;
        [SerializeField] private PipValuePanel pipValuePanel;
        [SerializeField] private ScarabSelectionPanel scarabSelectionPanel;
        [SerializeField] private PlayerSelectionPanel playerSelectionPanel;
        [SerializeField] private ContinueRollingPanel continueRollingPanel;

        // ── IPlayerInput implementation ───────────────────────────────────────

        /// <summary>Highlight active dice; player toggles selection; Done button confirms.</summary>
        public List<Die> ChooseDiceToLock(IReadOnlyList<Die> activeDice, Player player)
        {
            var tcs = new TaskCompletionSource<List<Die>>();
            UnityMainThread.Run(() =>
                diceSelectionPanel.Show(activeDice, player,
                    minSelect: 1, multiSelect: true,
                    onConfirm: selected => tcs.SetResult(selected)));
            return tcs.Task.GetAwaiter().GetResult();
        }

        /// <summary>Show Roll Again / Stop buttons; returns true to continue rolling.</summary>
        public bool ChooseContinueRolling(Player player)
        {
            var tcs = new TaskCompletionSource<bool>();
            UnityMainThread.Run(() =>
                continueRollingPanel.Show(player,
                    onResult: result => tcs.SetResult(result)));
            return tcs.Task.GetAwaiter().GetResult();
        }

        /// <summary>Highlight claimable market tiles; player clicks one or Pass.</summary>
        public Tile? ChooseTileToClaim(IReadOnlyList<Tile> claimable, Player player)
        {
            if (claimable.Count == 0) return null;
            var tcs = new TaskCompletionSource<Tile?>();
            UnityMainThread.Run(() =>
                tileSelectionPanel.Show(claimable, player,
                    allowSkip: true,
                    onResult: tile => tcs.SetResult(tile)));
            return tcs.Task.GetAwaiter().GetResult();
        }

        /// <summary>Highlight candidate dice; player clicks one (or skips).</summary>
        public Die? ChooseDie(IReadOnlyList<Die> dice, string prompt, Player player)
        {
            if (dice.Count == 0) return null;
            var tcs = new TaskCompletionSource<Die?>();
            UnityMainThread.Run(() =>
                diceSelectionPanel.ShowSingle(dice, player, prompt,
                    allowSkip: true,
                    onResult: die => tcs.SetResult(die)));
            return tcs.Task.GetAwaiter().GetResult();
        }

        /// <summary>Highlight candidate dice; player toggles a set, Done confirms.</summary>
        public List<Die> ChooseMultipleDice(IReadOnlyList<Die> dice, string prompt, Player player)
        {
            var tcs = new TaskCompletionSource<List<Die>>();
            UnityMainThread.Run(() =>
                diceSelectionPanel.Show(dice, player,
                    minSelect: 0, multiSelect: true,
                    headerText: prompt,
                    onConfirm: selected => tcs.SetResult(selected)));
            return tcs.Task.GetAwaiter().GetResult();
        }

        /// <summary>Number-picker popup 1–6.</summary>
        public int ChoosePipValue(Die die, string prompt, Player player)
        {
            var tcs = new TaskCompletionSource<int>();
            UnityMainThread.Run(() =>
                pipValuePanel.Show(die, prompt, player,
                    onResult: val => tcs.SetResult(val)));
            return tcs.Task.GetAwaiter().GetResult();
        }

        /// <summary>Show scarab panel; player clicks one or Pass.</summary>
        public Scarab? ChooseScarab(IReadOnlyList<Scarab> scarabs, Player player)
        {
            if (scarabs.Count == 0) return null;
            var tcs = new TaskCompletionSource<Scarab?>();
            UnityMainThread.Run(() =>
                scarabSelectionPanel.Show(scarabs, player,
                    onResult: s => tcs.SetResult(s)));
            return tcs.Task.GetAwaiter().GetResult();
        }

        /// <summary>Yes / No dialog.</summary>
        public bool ChooseYesNo(string prompt, Player player)
        {
            var tcs = new TaskCompletionSource<bool>();
            UnityMainThread.Run(() =>
                yesNoPanel.Show(prompt, player,
                    onResult: r => tcs.SetResult(r)));
            return tcs.Task.GetAwaiter().GetResult();
        }

        /// <summary>Glow-highlight the ability tile; player clicks to use or Skip.</summary>
        public bool ChooseUseAbility(Ability ability, Player player)
        {
            var tcs = new TaskCompletionSource<bool>();
            UnityMainThread.Run(() =>
                yesNoPanel.Show($"Use {ability.EntityName}?", player,
                    confirmLabel: "Use", cancelLabel: "Skip",
                    onResult: r => tcs.SetResult(r)));
            return tcs.Task.GetAwaiter().GetResult();
        }

        /// <summary>Show player portraits; player clicks one (or skips).</summary>
        public Player? ChoosePlayer(IReadOnlyList<Player> players, string prompt, Player activePlayer)
        {
            if (players.Count == 0) return null;
            var tcs = new TaskCompletionSource<Player?>();
            UnityMainThread.Run(() =>
                playerSelectionPanel.Show(players, prompt, activePlayer,
                    onResult: p => tcs.SetResult(p)));
            return tcs.Task.GetAwaiter().GetResult();
        }

        /// <summary>Show tile list; player clicks one (or skips).</summary>
        public Tile? ChooseTile(IReadOnlyList<Tile> tiles, string prompt, Player player)
        {
            if (tiles.Count == 0) return null;
            var tcs = new TaskCompletionSource<Tile?>();
            UnityMainThread.Run(() =>
                tileSelectionPanel.Show(tiles, player,
                    allowSkip: true,
                    headerText: prompt,
                    onResult: tile => tcs.SetResult(tile)));
            return tcs.Task.GetAwaiter().GetResult();
        }
    }
}
