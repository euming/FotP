using System.Collections.Generic;
using FotP.Engine.Dice;
using FotP.Engine.Tiles;

namespace FotP.Engine.Players
{
    /// <summary>
    /// Interface for player decisions. Console and Unity views implement this.
    /// </summary>
    public interface IPlayerInput
    {
        /// <summary>Choose which dice to lock from active dice. Must return at least one.</summary>
        List<Die> ChooseDiceToLock(IReadOnlyList<Die> activeDice, Player player);

        /// <summary>Choose whether to continue rolling (true) or stop and claim (false).</summary>
        bool ChooseContinueRolling(Player player);

        /// <summary>Choose a tile to claim from available options. May return null to skip.</summary>
        Tile? ChooseTileToClaim(IReadOnlyList<Tile> claimable, Player player);

        /// <summary>Choose a single die from a list (for abilities like adjust, reroll, etc).</summary>
        Die? ChooseDie(IReadOnlyList<Die> dice, string prompt, Player player);

        /// <summary>Choose multiple dice from a list.</summary>
        List<Die> ChooseMultipleDice(IReadOnlyList<Die> dice, string prompt, Player player);

        /// <summary>Choose a pip value (1-6) for a die.</summary>
        int ChoosePipValue(Die die, string prompt, Player player);

        /// <summary>Choose a scarab to use, or null to skip.</summary>
        Scarab? ChooseScarab(IReadOnlyList<Scarab> scarabs, Player player);

        /// <summary>Choose yes/no for a decision.</summary>
        bool ChooseYesNo(string prompt, Player player);

        /// <summary>Choose whether to use an ability.</summary>
        bool ChooseUseAbility(Ability ability, Player player);

        /// <summary>Choose a player from a list (e.g. for targeting abilities).</summary>
        Player? ChoosePlayer(IReadOnlyList<Player> players, string prompt, Player activePlayer);
    }
}
