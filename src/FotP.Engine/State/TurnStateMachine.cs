using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Dice;
using FotP.Engine.Players;
using FotP.Engine.Tiles;

namespace FotP.Engine.State
{
    /// <summary>
    /// Manages the flow of a single player's turn.
    /// </summary>
    public class TurnStateMachine
    {
        public TurnPhase Phase { get; private set; } = TurnPhase.StartOfTurn;
        public int RollCount { get; private set; }
        public DiceZoneManager Zones { get; }
        public Player? CurrentPlayer { get; private set; }

        private readonly Random _rng;

        public TurnStateMachine(Random rng)
        {
            _rng = rng;
            Zones = new DiceZoneManager();
        }

        /// <summary>Begin a new turn for the given player.</summary>
        public void BeginTurn(Player player, GameState state)
        {
            CurrentPlayer = player;
            RollCount = 0;
            Phase = TurnPhase.StartOfTurn;

            // Collect all dice to cup
            Zones.CollectAllToCup(player.DicePool);

            // Reset per-turn player state
            player.AdditionalClaims = 0;

            // Reset per-turn state on abilities
            foreach (var tile in player.OwnedTiles)
                foreach (var ability in tile.Abilities)
                    ability.ResetForTurn();

            // Fire StartOfTurn triggers
            FireTriggers(TriggerType.StartOfTurn, state, player);
        }

        /// <summary>Roll all dice in cup.</summary>
        public void PerformRoll(GameState state)
        {
            if (Phase != TurnPhase.StartOfTurn && Phase != TurnPhase.ContinueDecision)
                throw new InvalidOperationException($"Cannot roll in phase {Phase}");

            RollCount++;

            // Reset per-roll abilities
            foreach (var tile in CurrentPlayer!.OwnedTiles)
                foreach (var ability in tile.Abilities)
                    ability.ResetForRoll();

            Zones.RollAllInCup(_rng);
            Phase = TurnPhase.Rolling;

            // Auto-lock immediate dice
            Zones.AutoLockImmediateDice();

            // Fire AfterRoll triggers
            FireTriggers(TriggerType.AfterRoll, state, CurrentPlayer);

            Phase = TurnPhase.Locking;
        }

        /// <summary>Lock selected dice. Returns true if successful.</summary>
        public bool LockDice(List<Die> diceToLock, GameState state)
        {
            if (Phase != TurnPhase.Locking)
                throw new InvalidOperationException($"Cannot lock dice in phase {Phase}");

            if (diceToLock.Count == 0)
                throw new InvalidOperationException("Must lock at least one die.");

            foreach (var die in diceToLock)
                Zones.LockDie(die);

            // Fire LockedAny triggers
            FireTriggers(TriggerType.LockedAny, state, CurrentPlayer!);

            // Check if all dice are now locked
            if (Zones.Cup.Count == 0 && Zones.Active.Count == 0)
            {
                FireTriggers(TriggerType.AllLocked, state, CurrentPlayer!);
                Phase = TurnPhase.Claiming;
            }
            else
            {
                // Return remaining active dice to cup
                Zones.ReturnActiveToCup();
                Phase = TurnPhase.ScarabUse;
            }

            return true;
        }

        /// <summary>Transition from scarab use to continue decision.</summary>
        public void FinishScarabPhase()
        {
            if (Phase != TurnPhase.ScarabUse)
                throw new InvalidOperationException($"Cannot finish scarab phase in {Phase}");
            Phase = TurnPhase.ContinueDecision;
        }

        /// <summary>Player decides to stop rolling and claim.</summary>
        public void DecideToClaim()
        {
            if (Phase != TurnPhase.ContinueDecision)
                throw new InvalidOperationException($"Cannot claim in phase {Phase}");
            Phase = TurnPhase.Claiming;
        }

        /// <summary>Player decides to continue rolling.</summary>
        public void DecideToContinue()
        {
            if (Phase != TurnPhase.ContinueDecision)
                throw new InvalidOperationException($"Cannot continue in phase {Phase}");
            // Phase stays at ContinueDecision, caller will call PerformRoll
        }

        /// <summary>Reset phase to Claiming to allow an additional claim after PostClaim.</summary>
        public void ResetToClaimingPhase()
        {
            Phase = TurnPhase.Claiming;
        }

        /// <summary>Process tile claim.</summary>
        public void ClaimTile(Tile? tile, Player player, GameState state)
        {
            if (Phase != TurnPhase.Claiming)
                throw new InvalidOperationException($"Cannot claim in phase {Phase}");

            if (tile != null)
            {
                state.Market.ClaimTile(player, tile);
                FireTriggers(TriggerType.Acquire, state, player);
            }

            Phase = TurnPhase.PostClaim;
        }

        /// <summary>End the turn.</summary>
        public void EndTurn(GameState state)
        {
            FireTriggers(TriggerType.EndOfTurn, state, CurrentPlayer!);
            Phase = TurnPhase.EndOfTurn;
        }

        private void FireTriggers(TriggerType triggerType, GameState state, Player player)
        {
            var abilities = new List<Ability>();
            foreach (var tile in player.OwnedTiles)
            {
                foreach (var ability in tile.Abilities)
                {
                    if (ability.TriggerType == triggerType && ability.CanActivate(state, player))
                        abilities.Add(ability);
                }
            }

            foreach (var ability in abilities)
            {
                if (player.Input.ChooseUseAbility(ability, player))
                {
                    ability.Execute(state, player);
                    if (ability.IsPerTurn) ability.IsUsedThisTurn = true;
                    if (ability.IsPerRoll) ability.IsUsedThisRoll = true;
                    if (ability.IsArtifact && ability.ParentTile != null)
                        ability.ParentTile.IsArtifactUsed = true;
                }
            }
        }
    }
}
