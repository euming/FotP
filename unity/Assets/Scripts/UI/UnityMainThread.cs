using System;
using System.Collections.Generic;
using UnityEngine;

namespace FotP.Unity.UI
{
    /// <summary>
    /// Queues delegates to run on the Unity main thread.
    /// Add to a persistent GameObject early in scene load (e.g., via [RuntimeInitializeOnLoadMethod]).
    /// </summary>
    public class UnityMainThread : MonoBehaviour
    {
        private static UnityMainThread _instance;
        private readonly Queue<Action> _queue = new Queue<Action>();
        private readonly object _lock = new object();

        [RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.BeforeSceneLoad)]
        private static void Initialize()
        {
            var go = new GameObject("UnityMainThread");
            DontDestroyOnLoad(go);
            _instance = go.AddComponent<UnityMainThread>();
        }

        /// <summary>Enqueue an action to execute on the Unity main thread next Update.</summary>
        public static void Run(Action action)
        {
            if (_instance == null)
            {
                Debug.LogError("[UnityMainThread] Not initialized. Add to scene before use.");
                return;
            }
            lock (_instance._lock)
                _instance._queue.Enqueue(action);
        }

        private void Update()
        {
            while (true)
            {
                Action action;
                lock (_lock)
                {
                    if (_queue.Count == 0) break;
                    action = _queue.Dequeue();
                }
                action();
            }
        }
    }
}
