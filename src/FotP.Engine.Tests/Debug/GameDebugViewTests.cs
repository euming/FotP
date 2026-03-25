using FotP.Engine.Debug;
using FotP.Engine.Players;
using FotP.Engine.State;
using FotP.Engine.Tests.Helpers;

namespace FotP.Engine.Tests.Debug;

public class GameDebugViewTests
{
    [Fact]
    public void Render_ContainsPlayerNames()
    {
        var state = new GameState(new Random(1));
        state.Setup(new List<(string, IPlayerInput)>
        {
            ("Alice", new ScriptedPlayerInput()),
            ("Bob", new ScriptedPlayerInput())
        });

        var output = GameDebugView.Render(state);

        Assert.Contains("Alice", output);
        Assert.Contains("Bob", output);
    }

    [Fact]
    public void Render_ContainsRoundAndPhase()
    {
        var state = new GameState(new Random(1));
        state.Setup(new List<(string, IPlayerInput)>
        {
            ("Alice", new ScriptedPlayerInput())
        });

        var output = GameDebugView.Render(state);

        Assert.Contains("Round 1", output);
        Assert.Contains("Playing", output);
    }

    [Fact]
    public void Render_ContainsMarketTiles()
    {
        var state = new GameState(new Random(1));
        state.Setup(new List<(string, IPlayerInput)>
        {
            ("Alice", new ScriptedPlayerInput())
        });

        var output = GameDebugView.Render(state);

        Assert.Contains("Farmer", output);
        Assert.Contains("Guard", output);
    }

    [Fact]
    public void Render_ShowsTurnZonesAfterStartTurn()
    {
        var state = new GameState(new Random(1));
        state.Setup(new List<(string, IPlayerInput)>
        {
            ("Alice", new ScriptedPlayerInput())
        });
        state.StartTurn();

        var output = GameDebugView.Render(state);

        Assert.Contains("Cup", output);
        Assert.Contains("Alice", output);
    }
}
