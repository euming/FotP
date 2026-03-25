using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.Tiles;

namespace FotP.Engine.Tests.Helpers;

/// <summary>
/// A scripted IPlayerInput implementation for deterministic tests.
/// Each decision is driven by a queued callback or a simple default.
/// </summary>
public class ScriptedPlayerInput : IPlayerInput
{
    private readonly Queue<Func<IReadOnlyList<Die>, List<Die>>> _lockChoices = new();
    private bool _alwaysStop;
    private bool _alwaysClaim;
    private bool _neverUseAbility;

    public ScriptedPlayerInput(bool alwaysStop = true, bool alwaysClaim = false, bool neverUseAbility = true)
    {
        _alwaysStop = alwaysStop;
        _alwaysClaim = alwaysClaim;
        _neverUseAbility = neverUseAbility;
    }

    /// <summary>Queue a specific lock choice that runs once, then reverts to default.</summary>
    public void QueueLockChoice(Func<IReadOnlyList<Die>, List<Die>> chooser)
        => _lockChoices.Enqueue(chooser);

    public List<Die> ChooseDiceToLock(IReadOnlyList<Die> activeDice, Player player)
    {
        if (_lockChoices.Count > 0)
            return _lockChoices.Dequeue()(activeDice);
        // Default: lock the first die
        return activeDice.Take(1).ToList();
    }

    public bool ChooseContinueRolling(Player player) => !_alwaysStop;

    public Tile? ChooseTileToClaim(IReadOnlyList<Tile> claimable, Player player)
        => _alwaysClaim ? claimable.FirstOrDefault() : null;

    public Die? ChooseDie(IReadOnlyList<Die> dice, string prompt, Player player)
        => dice.FirstOrDefault();

    public List<Die> ChooseMultipleDice(IReadOnlyList<Die> dice, string prompt, Player player)
        => dice.Take(1).ToList();

    public int ChoosePipValue(Die die, string prompt, Player player) => 3;

    public Scarab? ChooseScarab(IReadOnlyList<Scarab> scarabs, Player player) => null;

    public bool ChooseYesNo(string prompt, Player player) => false;

    public bool ChooseUseAbility(Ability ability, Player player) => !_neverUseAbility;
}
