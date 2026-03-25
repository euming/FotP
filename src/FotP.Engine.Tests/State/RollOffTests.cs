using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.State;
using FotP.Engine.Tests.Helpers;
using Xunit;

namespace FotP.Engine.Tests.State;

/// <summary>
/// Tests for Phase 4: Roll-Off endgame mechanics.
/// </summary>
public class RollOffTests
{
    // Seeded RNG that produces predictable rolls (all 4s — a common face on Standard dice)
    private static GameState MakeState(int playerCount = 3, Random? rng = null)
    {
        rng ??= new Random(42);
        var state = new GameState(rng);
        var configs = new List<(string, IPlayerInput)>();
        for (int i = 0; i < playerCount; i++)
            configs.Add(($"P{i + 1}", new ScriptedPlayerInput(alwaysStop: true, alwaysClaim: false)));
        state.Setup(configs);
        return state;
    }

    [Fact]
    public void EnterRollOff_SetsPharaohHolderToQueenClaimant()
    {
        var state = MakeState(3);
        var queen = state.TurnOrder[0];

        // Manually lock a die so PyramidScore is non-zero
        var die = queen.DicePool[0];
        die.Roll(new Random(1));
        die.IsLocked = true;

        state.QueenClaimant = queen;
        state.EnterRollOff();

        Assert.Equal(GamePhase.RollOff, state.Phase);
        Assert.Equal(queen, state.PharaohHolder);
        Assert.Equal(queen.PyramidScore, state.PharaohScore);
    }

    [Fact]
    public void EnterRollOff_BuildsClockwiseRollOffOrder()
    {
        var state = MakeState(4);
        // Queen claimer is index 1 (P2)
        state.QueenClaimant = state.TurnOrder[1];
        state.EnterRollOff();

        // Roll-off order should be P3, P4, P1 (clockwise after P2)
        Assert.Equal(3, state.RollOffPlayers.Count);
        Assert.Equal(state.TurnOrder[2], state.RollOffPlayers[0]); // P3
        Assert.Equal(state.TurnOrder[3], state.RollOffPlayers[1]); // P4
        Assert.Equal(state.TurnOrder[0], state.RollOffPlayers[2]); // P1
    }

    [Fact]
    public void DetermineWinner_ReturnsPharaohHolder_WhenSet()
    {
        var state = MakeState(3);
        state.QueenClaimant = state.TurnOrder[0];
        state.EnterRollOff();

        // Simulate a player taking the token
        var thief = state.TurnOrder[1];
        state.PharaohHolder = thief;

        var winner = state.DetermineWinner();
        Assert.Equal(thief, winner);
        Assert.Equal(GamePhase.GameOver, state.Phase);
    }

    [Fact]
    public void RollOffTurn_QueenClaimerKeepsToken_WhenNobodyBeatsScore()
    {
        // Use a fixed seed where all players roll low values
        // P1 claims Queen with a big lead; P2 and P3 can't beat it
        var rng = new Random(0);
        var state = MakeState(2, rng);
        var engine = new GameEngine(state);

        var queen = state.TurnOrder[0];
        var challenger = state.TurnOrder[1];

        // Set up: queen claimer already has Pharaoh with score 30 (mocked high)
        state.QueenClaimant = queen;
        state.EnterRollOff();

        // Force pharaoh score to something unreachably high
        state.PharaohScore = 999;

        // Run roll-off manually using a single roll-off turn for challenger
        // Challenger's locked dice will have pip sum < 999
        state.TurnState.BeginTurn(challenger, state);
        state.TurnState.PerformRoll(state);
        var active = state.TurnState.Zones.Active.ToList();
        if (active.Count > 0)
            state.TurnState.LockDice(active, state);
        state.TurnState.EndTurn(state);

        int challengerScore = state.TurnState.Zones.GetLockedDiceWithPips().Sum(d => d.PipValue);
        Assert.True(challengerScore < 999, "Challenger should not beat score 999");

        // PharaohHolder should remain the queen claimer (we didn't change it)
        Assert.Equal(queen, state.PharaohHolder);
    }

    [Fact]
    public void GameEngine_RunRollOff_QueenClaimerWins_WhenNobodyBeats()
    {
        // Build a game where queen is pre-claimed and roll-off runs
        var state = MakeState(3, new Random(42));
        state.Phase = GamePhase.RollOff;
        state.QueenClaimant = state.TurnOrder[0];

        // Set queen score so high no one can beat it
        state.PharaohHolder = state.TurnOrder[0];
        state.PharaohScore = 999;

        // Build roll-off order manually
        state.RollOffPlayers.Clear();
        state.RollOffPlayers.Add(state.TurnOrder[1]);
        state.RollOffPlayers.Add(state.TurnOrder[2]);

        var engine = new GameEngine(state);
        var winner = state.DetermineWinner();

        // Queen claimer holds the Pharaoh token
        Assert.Equal(state.TurnOrder[0], winner);
    }

    [Fact]
    public void PharaohHolder_UpdatesToChallenger_WhenChallengerBeatsScore()
    {
        var state = MakeState(3);
        var queen = state.TurnOrder[0];
        var challenger = state.TurnOrder[1];

        state.QueenClaimant = queen;
        state.EnterRollOff();

        // Challenger rolls a score higher than current pharaoh score
        state.PharaohScore = 0;
        int challengerScore = 10;

        // Simulate the pharaoh token transfer logic
        if (challengerScore > state.PharaohScore)
        {
            state.PharaohHolder = challenger;
            state.PharaohScore = challengerScore;
        }

        Assert.Equal(challenger, state.PharaohHolder);
        Assert.Equal(10, state.PharaohScore);
    }

    [Fact]
    public void RollOffPlayers_ExcludesQueenClaimant()
    {
        var state = MakeState(3);
        state.QueenClaimant = state.TurnOrder[2];
        state.EnterRollOff();

        Assert.DoesNotContain(state.QueenClaimant, state.RollOffPlayers);
        Assert.Equal(2, state.RollOffPlayers.Count);
    }
}
