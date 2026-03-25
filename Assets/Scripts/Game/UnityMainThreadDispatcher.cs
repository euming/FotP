using System;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// Minimal main-thread dispatcher.  Engine callbacks arrive on a background
/// thread; views enqueue work here and it runs on the next Update().
///
/// Attach to any persistent GameObject (e.g. the same one as GameController).
/// </summary>
public class UnityMainThreadDispatcher : MonoBehaviour
{
    private static readonly Queue<Action> _queue = new();
    private static readonly object _lock = new();
    private static UnityMainThreadDispatcher _instance;

    void Awake()
    {
        if (_instance != null && _instance != this)
        {
            Destroy(gameObject);
            return;
        }
        _instance = this;
        DontDestroyOnLoad(gameObject);
    }

    void Update()
    {
        lock (_lock)
        {
            while (_queue.Count > 0)
                _queue.Dequeue()?.Invoke();
        }
    }

    /// <summary>Enqueue an action to run on the Unity main thread.</summary>
    public static void Enqueue(Action action)
    {
        if (action == null) return;
        lock (_lock)
        {
            _queue.Enqueue(action);
        }
    }
}
