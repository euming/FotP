using FotP.Engine.Tiles;

namespace FotP.Engine.Market
{
    /// <summary>
    /// A stack of identical tiles in the market bar.
    /// </summary>
    public class TileStack
    {
        public Tile Prototype { get; }
        public int Remaining { get; set; }
        public int SlotIndex { get; }

        public TileStack(Tile prototype, int count, int slotIndex)
        {
            Prototype = prototype;
            Remaining = count;
            SlotIndex = slotIndex;
        }

        public bool IsEmpty => Remaining <= 0;

        public override string ToString() => $"{Prototype.Name} x{Remaining} (L{Prototype.Level})";
    }
}
