using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using FotP.Engine.State;
using FotP.Engine.Players;
using FotP.Engine.Market;

/// <summary>
/// Central MonoBehaviour that owns GameState and drives the game loop.
/// Lives in the Game scene. Set up player configs and call InitGame() before use.
/// </summary>
public class GameController : MonoBehaviour
{
    public static GameController Instance { get; private set; }

    public GameState State { get; private set; }

    // Events that views subscribe to
    public event System.Action<Player> OnTurnStarted;
    public event System.Action OnGameOver;

    [SerializeField] private int playerCount = 2;
    [SerializeField] private string resultsSceneName = "Results";

    void Awake()
    {
        if (Instance != null && Instance != this)
        {
            Destroy(gameObject);
            return;
        }
        Instance = this;
        DontDestroyOnLoad(gameObject);
    }

    void Start()
    {
        // Override serialized default with player count chosen on the main menu (if set).
        if (PlayerPrefs.HasKey("SelectedPlayerCount"))
            playerCount = PlayerPrefs.GetInt("SelectedPlayerCount");

        InitGame();
    }

    public void InitGame()
    {
        State = new GameState();

        // UnityPlayerInput will be implemented in a later sprint.
        // For now we pass null inputs so the engine can be verified loaded.
        var configs = new List<(string name, IPlayerInput input)>();
        for (int i = 0; i < playerCount; i++)
            configs.Add(($"Player {i + 1}", null));

        State.Setup(configs);
        Debug.Log($"[GameController] GameState initialized. Phase={State.Phase}, Players={State.TurnOrder.Count}");
    }

    /// <summary>
    /// Call when the engine signals game over. Fires OnGameOver event then loads Results scene.
    /// </summary>
    public void HandleGameOver()
    {
        OnGameOver?.Invoke();
        Debug.Log("[GameController] Game over — loading Results scene.");
        SceneManager.LoadScene(resultsSceneName);
    }
}
