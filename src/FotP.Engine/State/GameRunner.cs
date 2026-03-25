using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Market;
using FotP.Engine.Players;
using FotP.Engine.Tiles;

namespace FotP.Engine.State
{
    /// <summary>
    /// Orchestrates a full game from setup to completion.
    /// </summary>
    public class GameRunner
    {
        private readonly GameState _state;
        private readonly int _maxRounds;

        public GameState State => _state;

        public GameRunner(GameState state, int maxRounds = 50)
        {
            _state = state;
            _maxRounds = maxRounds;
        }

        /// <summary>Run a full game. Returns the winner.</summary>
        public Player RunGame()
        {
            while (_state.Phase == GamePhase.Playing && _state.RoundNumber <= _maxRounds)
            {
                RunTurn(_state.CurrentPlayer!);

                // Check if Queen was claimed
                if (_state.QueenClaimant != null)
                {
                    // Remaining players get one more turn
                    FinishRound();
                    return RunRollOff();
                }

                _state.NextPlayer();
            }

            return _state.DetermineWinner();
        }

        private void RunTurn(Player player)
        {
            _state.StartTurn();

            // Roll phase
            _state.TurnState.PerformRoll(_state);

            // Main loop: lock dice, optionally continue
            while (true)
            {
                var activeDice = _state.TurnState.Zones.Active.ToList();
                if (activeDice.Count == 0)
                {
                    // All dice locked via auto-lock or no dice left
                    if (_state.TurnState.Phase == TurnPhase.Claiming)
                        break;
                    // Force to claiming if nothing to lock
                    if (_state.TurnState.Phase == TurnPhase.Locking)
                    {
                        // No active dice but there might be cup dice
                        if (_state.TurnState.Zones.Cup.Count > 0)
                        {
                            _state.TurnState.FinishScarabPhase();
                            break; // Will go to claim
                        }
                    }
                    break;
                }

                // Choose dice to lock
                var diceToLock = player.Input.ChooseDiceToLock(activeDice, player);
                if (diceToLock.Count == 0)
                    diceToLock = new List<Die> { activeDice[0] }; // Must lock at least one

                _state.TurnState.LockDice(diceToLock, _state);

                if (_state.TurnState.Phase == TurnPhase.Claiming)
                    break; // All dice locked

                // Scarab phase
                RunScarabPhase(player);

                // Continue decision
                bool continueRolling = _state.TurnState.Zones.Cup.Count > 0 &&
                                       player.Input.ChooseContinueRolling(player);

                if (continueRolling)
                {
                    _state.TurnState.DecideToContinue();
                    _state.TurnState.PerformRoll(_state);
                }
                else
                {
                    _state.TurnState.DecideToClaim();
                    break;
                }
            }

            // Claiming phase
            var lockedDice = _state.TurnState.Zones.GetAllLockedDice();
            var claimable = _state.Market.GetClaimableStacks(player, lockedDice);
            Tile? chosenTile = null;

            if (claimable.Count > 0)
            {
                var claimableTiles = claimable.Select(s => s.Prototype).ToList();
                chosenTile = player.Input.ChooseTileToClaim(claimableTiles, player);
            }

            _state.TurnState.ClaimTile(chosenTile, player, _state);

            // Check for Queen
            if (chosenTile?.Name == "Queen")
                _state.QueenClaimant = player;

            _state.TurnState.EndTurn(_state);
        }

        private void RunScarabPhase(Player player)
        {
            // Allow the player to use any number of scarabs they own
            while (player.Scarabs.Count > 0)
            {
                var chosen = player.Input.ChooseScarab(player.Scarabs, player);
                if (chosen == null)
                    break;

                // Remove the scarab before applying (it's consumed)
                player.Scarabs.Remove(chosen);

                var lockedDice = _state.TurnState.Zones.GetAllLockedDice();
                var cupList = _state.TurnState.Zones.Cup;

                Die? target = null;
                if (chosen.Type == ScarabType.Reroll || chosen.Type == ScarabType.Pip)
                {
                    if (lockedDice.Count > 0)
                        target = player.Input.ChooseDie(lockedDice, $"Choose a die for {chosen.Type} scarab", player);
                }

                chosen.Apply(target, _state.Rng, cupList);
            }

            _state.TurnState.FinishScarabPhase();
        }

        private void FinishRound()
        {
            // Give remaining players in the round one more turn
            int startIdx = (_state.CurrentPlayerIndex + 1) % _state.TurnOrder.Count;
            int queenIdx = _state.TurnOrder.ToList().IndexOf(_state.QueenClaimant!);

            int idx = startIdx;
            while (idx != queenIdx)
            {
                var player = _state.TurnOrder[idx];
                RunTurn(player);
                idx = (idx + 1) % _state.TurnOrder.Count;
                if (idx == startIdx) break; // Safety
            }
        }

        private Player RunRollOff()
        {
            _state.EnterRollOff();

            // Each roll-off player rolls all their dice once.
            // If their pyramid score >= PharaohScore, they take the Pharaoh token.
            foreach (var player in _state.RollOffPlayers)
            {
                foreach (var die in player.DicePool)
                {
                    die.IsLocked = false;
                    die.TempPipModifier = 0;
                    die.Roll(_state.Rng);
                    die.IsLocked = true;
                }

                int score = player.PyramidScore;
                if (score >= _state.PharaohScore)
                {
                    _state.PharaohHolder = player;
                    _state.PharaohScore = score;
                }
            }

            return _state.DetermineWinner();
        }
    }
}
