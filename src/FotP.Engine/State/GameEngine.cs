using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.Tiles;

namespace FotP.Engine.State
{
    /// <summary>
    /// Drives the full game loop. Calls into GameState and TurnStateMachine.
    /// Pure C# — no Unity dependency.
    /// </summary>
    public class GameEngine
    {
        public GameState State { get; }

        public GameEngine(GameState state)
        {
            State = state;
        }

        /// <summary>
        /// Run a complete game to completion. Returns the winning player.
        /// </summary>
        public Player RunGame()
        {
            if (State.Phase != GamePhase.Playing)
                throw new InvalidOperationException("Game must be in Playing phase to run.");

            while (State.Phase == GamePhase.Playing)
            {
                RunPlayerTurn(State.CurrentPlayer!);

                if (State.Phase == GamePhase.RollOff)
                {
                    // Remaining players in this round get one final normal turn before roll-off
                    CompleteRoundAfterQueenClaim();
                    break;
                }

                State.NextPlayer();
            }

            if (State.Phase == GamePhase.RollOff)
                RunRollOff();

            return State.DetermineWinner();
        }

        /// <summary>
        /// After the queen claimer's turn ends the round, players later in turn order
        /// who haven't had their turn this round each get one normal turn.
        /// </summary>
        private void CompleteRoundAfterQueenClaim()
        {
            var ordered = State.TurnOrder.ToList();
            int queenIdx = ordered.IndexOf(State.QueenClaimant!);

            for (int i = queenIdx + 1; i < ordered.Count; i++)
                RunPlayerTurn(ordered[i]);
        }

        /// <summary>
        /// Execute one full player turn: start -> rolls -> claim -> end.
        /// </summary>
        public void RunPlayerTurn(Player player)
        {
            State.TurnState.BeginTurn(player, State);

            RunRollLoop(player);

            RunClaimPhase(player);

            State.TurnState.EndTurn(State);

            // Check end-game condition after turn
            if (State.QueenClaimant != null && State.Phase == GamePhase.Playing)
            {
                State.EnterRollOff();
            }
        }

        /// <summary>
        /// Inner roll loop: roll -> lock -> optional scarabs -> decide to continue or stop.
        /// </summary>
        private void RunRollLoop(Player player)
        {
            while (true)
            {
                State.TurnState.PerformRoll(State);

                // Player must lock at least one die
                var activeDice = State.TurnState.Zones.Active.ToList();
                if (activeDice.Count == 0)
                {
                    // All dice auto-locked (immediate dice), go straight to claim
                    break;
                }

                var tolock = player.Input.ChooseDiceToLock(activeDice, player);
                State.TurnState.LockDice(tolock, State);

                // If all dice locked, loop exits
                if (State.TurnState.Phase == TurnPhase.Claiming)
                    break;

                // Scarab phase (optional)
                RunScarabPhase(player);
                State.TurnState.FinishScarabPhase();

                // Continue decision
                bool continueRolling = player.Input.ChooseContinueRolling(player);
                if (continueRolling)
                    State.TurnState.DecideToContinue();
                else
                {
                    State.TurnState.DecideToClaim();
                    break;
                }
            }
        }

        /// <summary>
        /// Optional scarab spending phase.
        /// </summary>
        private void RunScarabPhase(Player player)
        {
            if (player.Scarabs.Count == 0) return;

            var scarab = player.Input.ChooseScarab(player.Scarabs.ToList(), player);
            while (scarab != null)
            {
                player.Scarabs.Remove(scarab);
                ApplyScarab(scarab, player);

                if (player.Scarabs.Count == 0) break;
                scarab = player.Input.ChooseScarab(player.Scarabs.ToList(), player);
            }
        }

        private void ApplyScarab(Scarab scarab, Player player)
        {
            var zones = State.TurnState.Zones;
            var activeDice = zones.Cup.ToList(); // Scarabs apply to dice in cup

            switch (scarab.Type)
            {
                case ScarabType.Reroll:
                    var rerollTarget = activeDice.Count > 0
                        ? player.Input.ChooseDie(activeDice, "Scarab: Choose a die to reroll", player)
                        : null;
                    if (rerollTarget != null)
                        scarab.Apply(rerollTarget, State.Rng);
                    break;

                case ScarabType.Pip:
                    var pipTarget = activeDice.Count > 0
                        ? player.Input.ChooseDie(activeDice, "Scarab: Choose a die to add +1 pip", player)
                        : null;
                    if (pipTarget != null)
                        scarab.Apply(pipTarget, State.Rng);
                    break;

                case ScarabType.Die:
                    var newDie = scarab.Apply(null, State.Rng, zones.Cup);
                    if (newDie != null)
                    {
                        player.DicePool.Add(newDie);
                        zones.Temporary.Add(newDie);
                    }
                    break;
            }
        }

        /// <summary>
        /// Claim phase: player may claim one tile or gain 2 scarabs.
        /// </summary>
        private void RunClaimPhase(Player player)
        {
            var lockedDice = State.TurnState.Zones.GetAllLockedDice();
            var claimable = State.Market.GetClaimableStacks(player, lockedDice);

            Tile? chosen = null;
            if (claimable.Count > 0)
            {
                var claimableTiles = claimable.Select(s => s.Prototype).ToList();
                chosen = player.Input.ChooseTileToClaim(claimableTiles, player);
            }

            if (chosen != null)
            {
                State.TurnState.ClaimTile(chosen, player, State);

                // Check if Queen was claimed
                if (chosen.Name == "Queen")
                    State.QueenClaimant = player;
            }
            else
            {
                // No claim — gain 2 scarab tokens
                player.Scarabs.Add(new Scarab(ScarabType.Reroll));
                player.Scarabs.Add(new Scarab(ScarabType.Reroll));
                State.TurnState.ClaimTile(null, player, State);
            }
        }

        /// <summary>
        /// Roll-off: each non-queen-claimer player (clockwise from next after queen claimer)
        /// gets one full roll-off turn. If anyone takes the Pharaoh token, the queen claimer
        /// gets one final chance to reclaim it.
        /// </summary>
        private void RunRollOff()
        {
            // 1. Royal Death integration: if RollOffBarScore is set and higher, use it as the floor
            if (State.RollOffBarScore.HasValue && State.RollOffBarScore.Value > State.PharaohScore)
                State.PharaohScore = State.RollOffBarScore.Value;

            // 3. Compensation dice: players who already had their turn this round (before queen
            //    claimer in turn order) missed the chance to play knowing it was the final round.
            //    Give each of them +1 standard die for the roll-off.
            var ordered = State.TurnOrder.ToList();
            int queenIdx = ordered.IndexOf(State.QueenClaimant!);
            for (int i = 0; i < queenIdx; i++)
                ordered[i].DicePool.Add(new Die(DieType.Standard));

            bool tokenLeftQueenClaimer = false;

            // Each roll-off player gets one full turn (roll/lock, no claim)
            foreach (var player in State.RollOffPlayers)
            {
                // 2. Eliminated player skip: if max possible score (all 6s) can't beat the bar, skip
                int maxPossible = player.DicePool.Count * 6;
                if (maxPossible < State.PharaohScore)
                    continue;

                int score = RunRollOffTurn(player);

                // Take the Pharaoh token by matching or beating the current score
                if (score >= State.PharaohScore)
                {
                    State.PharaohHolder = player;
                    State.PharaohScore = score;
                    tokenLeftQueenClaimer = true;
                }
            }

            // 4. Queen's Last Chance: if the token changed hands, queen claimer gets one final reclaim attempt
            if (tokenLeftQueenClaimer && State.QueenClaimant != null)
            {
                int score = RunRollOffTurn(State.QueenClaimant);
                if (score >= State.PharaohScore)
                {
                    State.PharaohHolder = State.QueenClaimant;
                    State.PharaohScore = score;
                }
            }
        }

        /// <summary>
        /// One roll-off turn: full dice rolling and locking (with scarabs and continue decisions),
        /// but no tile claiming. Returns the pip sum of locked dice.
        /// </summary>
        private int RunRollOffTurn(Player player)
        {
            State.TurnState.BeginTurn(player, State);
            RunRollLoop(player);
            State.TurnState.EndTurn(State);

            return State.TurnState.Zones.GetLockedDiceWithPips().Sum(d => d.PipValue);
        }
    }
}
