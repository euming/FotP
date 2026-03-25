using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using UnityEngine.UI;
using FotP.Engine.Market;

namespace FotP.View
{
    /// <summary>
    /// Displays one level-bar of the tile market (e.g. all Level-3 tiles).
    /// Instantiates a <see cref="MarketSlotView"/> prefab for each TileStack
    /// that belongs to this level, laying them out horizontally inside a layout group.
    /// </summary>
    public class MarketBarView : MonoBehaviour
    {
        [Header("Data")]
        [Tooltip("Market level this bar represents (3-7)")]
        public int level;

        [Header("UI References")]
        [Tooltip("Label showing the bar level (e.g. 'Level 3')")]
        public Text levelLabel;

        [Tooltip("Container with a HorizontalLayoutGroup for slot children")]
        public Transform slotContainer;

        [Header("Prefabs")]
        [Tooltip("Prefab instantiated for each tile slot in this bar")]
        public MarketSlotView slotPrefab;

        private readonly List<MarketSlotView> _slotViews = new();

        /// <summary>
        /// Build the bar from the given stacks (all must belong to <see cref="level"/>).
        /// Clears any previously created slots first.
        /// </summary>
        public void Build(IEnumerable<TileStack> stacksForThisLevel)
        {
            ClearSlots();

            if (levelLabel != null)
                levelLabel.text = level == 7 ? "Queen (L7)" : $"Level {level}";

            foreach (var stack in stacksForThisLevel.OrderBy(s => s.SlotIndex))
            {
                if (slotPrefab == null || slotContainer == null) break;

                var slotGo = Instantiate(slotPrefab.gameObject, slotContainer);
                var slotView = slotGo.GetComponent<MarketSlotView>();
                if (slotView != null)
                {
                    slotView.Bind(stack);
                    _slotViews.Add(slotView);
                }
            }
        }

        /// <summary>
        /// Refresh all slot labels (call after a tile is claimed from this bar).
        /// </summary>
        public void Refresh()
        {
            foreach (var sv in _slotViews)
                sv.Refresh();
        }

        private void ClearSlots()
        {
            foreach (var sv in _slotViews)
            {
                if (sv != null)
                    Destroy(sv.gameObject);
            }
            _slotViews.Clear();
        }
    }
}
