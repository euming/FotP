using System.Collections.Generic;
using System.Threading.Tasks;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.Tiles;

/// <summary>
/// Unity implementation of IPlayerInput.
/// Each method blocks the engine coroutine thread via Task.Run until the UI resolves
/// a TaskCompletionSource.  UI components call the Resolve* methods to supply answers.
/// </summary>
public class UnityPlayerInput : IPlayerInput
{
    // -----------------------------------------------------------------------
    // Pending task slots — only one decision is outstanding at a time
    // -----------------------------------------------------------------------
    private TaskCompletionSource<List<Die>>  _lockTcs;
    private TaskCompletionSource<bool>       _continueTcs;
    private TaskCompletionSource<Tile>       _claimTileTcs;
    private TaskCompletionSource<Die>        _dieTcs;
    private TaskCompletionSource<List<Die>>  _multiDieTcs;
    private TaskCompletionSource<int>        _pipTcs;
    private TaskCompletionSource<Scarab>     _scarabTcs;
    private TaskCompletionSource<bool>       _yesNoTcs;
    private TaskCompletionSource<bool>       _abilityTcs;
    private TaskCompletionSource<Player>     _playerTcs;
    private TaskCompletionSource<Tile>       _tileTcs;

    // -----------------------------------------------------------------------
    // Properties exposed to UI panels so they know what to display
    // -----------------------------------------------------------------------
    public IReadOnlyList<Die>  PendingActiveDice  { get; private set; }
    public IReadOnlyList<Die>  PendingDiceList    { get; private set; }
    public IReadOnlyList<Tile> PendingTileList    { get; private set; }
    public IReadOnlyList<Scarab> PendingScarabs   { get; private set; }
    public IReadOnlyList<Player> PendingPlayers   { get; private set; }
    public string              PendingPrompt      { get; private set; }
    public Die                 PendingDie         { get; private set; }
    public Ability             PendingAbility     { get; private set; }
    public Player              PendingPlayer      { get; private set; }

    // -----------------------------------------------------------------------
    // IPlayerInput — called from background Task (engine thread)
    // -----------------------------------------------------------------------

    public List<Die> ChooseDiceToLock(IReadOnlyList<Die> activeDice, Player player)
    {
        PendingActiveDice = activeDice;
        PendingPlayer     = player;
        _lockTcs          = new TaskCompletionSource<List<Die>>();
        return _lockTcs.Task.GetAwaiter().GetResult();
    }

    public bool ChooseContinueRolling(Player player)
    {
        PendingPlayer = player;
        _continueTcs  = new TaskCompletionSource<bool>();
        return _continueTcs.Task.GetAwaiter().GetResult();
    }

    public Tile ChooseTileToClaim(IReadOnlyList<Tile> claimable, Player player)
    {
        PendingTileList = claimable;
        PendingPlayer   = player;
        _claimTileTcs   = new TaskCompletionSource<Tile>();
        return _claimTileTcs.Task.GetAwaiter().GetResult();
    }

    public Die ChooseDie(IReadOnlyList<Die> dice, string prompt, Player player)
    {
        PendingDiceList = dice;
        PendingPrompt   = prompt;
        PendingPlayer   = player;
        _dieTcs         = new TaskCompletionSource<Die>();
        return _dieTcs.Task.GetAwaiter().GetResult();
    }

    public List<Die> ChooseMultipleDice(IReadOnlyList<Die> dice, string prompt, Player player)
    {
        PendingDiceList = dice;
        PendingPrompt   = prompt;
        PendingPlayer   = player;
        _multiDieTcs    = new TaskCompletionSource<List<Die>>();
        return _multiDieTcs.Task.GetAwaiter().GetResult();
    }

    public int ChoosePipValue(Die die, string prompt, Player player)
    {
        PendingDie    = die;
        PendingPrompt = prompt;
        PendingPlayer = player;
        _pipTcs       = new TaskCompletionSource<int>();
        return _pipTcs.Task.GetAwaiter().GetResult();
    }

    public Scarab ChooseScarab(IReadOnlyList<Scarab> scarabs, Player player)
    {
        PendingScarabs = scarabs;
        PendingPlayer  = player;
        _scarabTcs     = new TaskCompletionSource<Scarab>();
        return _scarabTcs.Task.GetAwaiter().GetResult();
    }

    public bool ChooseYesNo(string prompt, Player player)
    {
        PendingPrompt = prompt;
        PendingPlayer = player;
        _yesNoTcs     = new TaskCompletionSource<bool>();
        return _yesNoTcs.Task.GetAwaiter().GetResult();
    }

    public bool ChooseUseAbility(Ability ability, Player player)
    {
        PendingAbility = ability;
        PendingPlayer  = player;
        _abilityTcs    = new TaskCompletionSource<bool>();
        return _abilityTcs.Task.GetAwaiter().GetResult();
    }

    public Player ChoosePlayer(IReadOnlyList<Player> players, string prompt, Player activePlayer)
    {
        PendingPlayers = players;
        PendingPrompt  = prompt;
        PendingPlayer  = activePlayer;
        _playerTcs     = new TaskCompletionSource<Player>();
        return _playerTcs.Task.GetAwaiter().GetResult();
    }

    public Tile ChooseTile(IReadOnlyList<Tile> tiles, string prompt, Player player)
    {
        PendingTileList = tiles;
        PendingPrompt   = prompt;
        PendingPlayer   = player;
        _tileTcs        = new TaskCompletionSource<Tile>();
        return _tileTcs.Task.GetAwaiter().GetResult();
    }

    // -----------------------------------------------------------------------
    // Resolve methods — called by UI panels on the Unity main thread
    // -----------------------------------------------------------------------

    public void ResolveDiceToLock(List<Die> dice)         => _lockTcs?.TrySetResult(dice);
    public void ResolveContinueRolling(bool cont)          => _continueTcs?.TrySetResult(cont);
    public void ResolveClaimTile(Tile tile)                => _claimTileTcs?.TrySetResult(tile);
    public void ResolveDie(Die die)                        => _dieTcs?.TrySetResult(die);
    public void ResolveMultipleDice(List<Die> dice)        => _multiDieTcs?.TrySetResult(dice);
    public void ResolvePipValue(int pip)                   => _pipTcs?.TrySetResult(pip);
    public void ResolveScarab(Scarab scarab)               => _scarabTcs?.TrySetResult(scarab);
    public void ResolveYesNo(bool answer)                  => _yesNoTcs?.TrySetResult(answer);
    public void ResolveUseAbility(bool use)                => _abilityTcs?.TrySetResult(use);
    public void ResolvePlayer(Player player)               => _playerTcs?.TrySetResult(player);
    public void ResolveTile(Tile tile)                     => _tileTcs?.TrySetResult(tile);
}
