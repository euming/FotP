using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using FotP.Engine.Players;
using FotP.Engine.State;
using UnityEngine;

/// <summary>
/// Top-level MonoBehaviour that bootstraps FotP.Engine and drives the game loop.
///
/// Attach to a persistent GameObject in the main game scene.  Configure
/// playerNames in the Inspector; one UnityPlayerInput is created per player.
///
/// The engine's synchronous RunGame() runs on a background thread so it can
/// block on TaskCompletionSource without stalling the Unity main thread.
/// UI panels call the Resolve* methods on the active player's Input to unblock
/// each decision.
/// </summary>
public class GameController : MonoBehaviour
{
    // -----------------------------------------------------------------------
    // Inspector fields
    // -----------------------------------------------------------------------

    [Tooltip("Names for each human player (2–4).")]
    public List<string> playerNames = new() { "Player 1", "Player 2" };

    [Tooltip("Number of starting dice per player.")]
    public int startingDice = 3;

    // -----------------------------------------------------------------------
    // Runtime state
    // -----------------------------------------------------------------------

    /// <summary>The running engine instance. Null until StartGame is called.</summary>
    public GameEngine Engine { get; private set; }

    /// <summary>Per-player Unity inputs, indexed by turn order.</summary>
    public List<UnityPlayerInput> PlayerInputs { get; } = new();

    /// <summary>Fired when the game finishes. Carries the winner name.</summary>
    public event Action<string> OnGameOver;

    // -----------------------------------------------------------------------
    // Unity lifecycle
    // -----------------------------------------------------------------------

    void Start()
    {
        StartGame();
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// <summary>
    /// Initialise a new game and start the engine loop on a background thread.
    /// </summary>
    public void StartGame()
    {
        if (playerNames == null || playerNames.Count < 2)
        {
            Debug.LogError("GameController: need at least 2 player names.");
            return;
        }

        PlayerInputs.Clear();

        var state = new FotP.Engine.State.GameState();
        var configs = new List<(string, IPlayerInput)>();

        foreach (var name in playerNames)
        {
            var input = new UnityPlayerInput();
            PlayerInputs.Add(input);
            configs.Add((name, input));
        }

        state.Setup(configs, startingDice);
        state.Phase = GamePhase.Playing;

        Engine = new GameEngine(state);

        // Run blocking engine on a thread-pool thread so Unity main thread stays free.
        Task.Run(() =>
        {
            try
            {
                var winner = Engine.RunGame();
                // Marshal result back to Unity main thread via a coroutine flag.
                _winnerName = winner.Name;
                _gameOver   = true;
            }
            catch (Exception ex)
            {
                Debug.LogError($"GameController: engine exception: {ex}");
                _gameOver   = true;
                _winnerName = "Error";
            }
        });
    }

    // -----------------------------------------------------------------------
    // Convenience accessors for UI
    // -----------------------------------------------------------------------

    /// <summary>
    /// Returns the UnityPlayerInput for the player whose turn it currently is,
    /// or null if the game has not started.
    /// </summary>
    public UnityPlayerInput CurrentInput
    {
        get
        {
            if (Engine == null) return null;
            var cp = Engine.State.CurrentPlayer;
            if (cp == null) return null;
            int idx = Engine.State.TurnOrder.IndexOf(cp);
            return idx >= 0 && idx < PlayerInputs.Count ? PlayerInputs[idx] : null;
        }
    }

    // -----------------------------------------------------------------------
    // Internal — bridge background thread result to Unity main thread
    // -----------------------------------------------------------------------

    private volatile bool   _gameOver   = false;
    private volatile string _winnerName = null;

    void Update()
    {
        if (_gameOver)
        {
            _gameOver = false;
            Debug.Log($"Game over! Winner: {_winnerName}");
            OnGameOver?.Invoke(_winnerName);
        }
    }
}
