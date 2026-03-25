using FotP.Engine.Tiles;

namespace FotP.Engine.Market;

/// <summary>
/// Describes a bar slot's tile requirement for the dealer.
/// FixedTileId is non-null for first-game (A-side) setup; null for random dealing.
/// </summary>
public record BarSlotRequest(int Level, TileColor Color, string? FixedTileId = null);
