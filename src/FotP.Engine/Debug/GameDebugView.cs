using System.Text;
using FotP.Engine.Dice;
using FotP.Engine.State;

namespace FotP.Engine.Debug
{
    /// <summary>
    /// Produces a human-readable text dump of the current game state.
    /// Used in console output and test assertions.
    /// </summary>
    public static class GameDebugView
    {
        public static string Render(GameState state)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"=== Game State [Round {state.RoundNumber}  Phase: {state.Phase}] ===");
            sb.AppendLine();

            // Players
            foreach (var player in state.TurnOrder)
            {
                string marker = player == state.CurrentPlayer ? ">> " : "   ";
                sb.AppendLine($"{marker}Player: {player.Name}  Tokens:{player.Tokens}  PyramidScore:{player.PyramidScore}");
                sb.Append("      Dice: ");
                foreach (var die in player.DicePool)
                    sb.Append($"[{die}] ");
                sb.AppendLine();
                if (player.OwnedTiles.Count > 0)
                {
                    sb.Append("      Tiles: ");
                    foreach (var tile in player.OwnedTiles)
                        sb.Append($"{tile.Name}({tile.Color}L{tile.Level}) ");
                    sb.AppendLine();
                }
            }
            sb.AppendLine();

            // Turn state
            var ts = state.TurnState;
            if (ts.CurrentPlayer != null)
            {
                sb.AppendLine($"  Turn: {ts.CurrentPlayer.Name}  Phase:{ts.Phase}  Rolls:{ts.RollCount}");
                AppendZone(sb, "Cup    ", ts.Zones.Cup);
                AppendZone(sb, "Active ", ts.Zones.Active);
                AppendZone(sb, "Locked ", ts.Zones.Locked);
                if (ts.Zones.SetAside.Count > 0)
                    AppendZone(sb, "SetAsid", ts.Zones.SetAside);
                sb.AppendLine();
            }

            // Market summary
            sb.AppendLine("  Market:");
            foreach (var stack in state.Market.Stacks)
            {
                if (!stack.IsEmpty)
                {
                    var t = stack.Prototype;
                    string criteria = t.ClaimCriteria?.Description ?? "none";
                    sb.AppendLine($"    [{stack.Remaining,2}x] {t.Name,-18} L{t.Level} {t.Color,-8} Criteria:{criteria}");
                }
            }

            return sb.ToString();
        }

        private static void AppendZone(StringBuilder sb, string label, IEnumerable<Die> dice)
        {
            sb.Append($"    {label}: ");
            foreach (var d in dice)
                sb.Append($"[{d}] ");
            sb.AppendLine();
        }
    }
}
