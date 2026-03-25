namespace FotP.Engine.State
{
    public enum GamePhase
    {
        Setup,
        Playing,
        RollOff,
        GameOver
    }

    public enum TurnPhase
    {
        StartOfTurn,
        Rolling,
        Locking,
        ScarabUse,
        ContinueDecision,
        Claiming,
        PostClaim,
        EndOfTurn
    }
}
