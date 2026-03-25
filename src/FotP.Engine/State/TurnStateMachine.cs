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

            // Apply any dice modifier from previous turn (e.g. Bad Omen)
            int diceModifier = player.StandardDiceModifierNextTurn;
            player.StandardDiceModifierNextTurn = 0;
            if (diceModifier < 0)
            {
                int toRemove = System.Math.Min(-diceModifier, Zones.Cup.Count - 1);
                for (int i = 0; i < toRemove; i++)
                {
                    var stdDie = Zones.Cup.FirstOrDefault(d => d.DieType == DieType.Standard);
                    if (stdDie == null) break;
                    Zones.Cup.Remove(stdDie);
                    player.DicePool.Remove(stdDie);
                }
            }
            else if (diceModifier > 0)
            {
                for (int i = 0; i < diceModifier; i++)
                {
                    var die = new Die(DieType.Standard) { IsTemporary = true };
                    player.DicePool.Add(die);
                    Zones.Cup.Add(die);
                    Zones.Temporary.Add(die);
                }
            }

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

            // Fire custom dice face abilities (Artisan *, Intrigue **, Voyage faces, Decree *)
            FireCustomDiceFaceAbilities(state, CurrentPlayer);

            // Fire AfterRoll triggers
            FireTriggers(TriggerType.AfterRoll, state, CurrentPlayer);

            // If all dice are now locked (e.g. TombBuilder locked the last die), skip to Claiming
            if (Zones.Active.Count == 0 && Zones.Cup.Count == 0)
            {
                FireTriggers(TriggerType.AllLocked, state, CurrentPlayer!);
                Phase = TurnPhase.Claiming;
            }
            else
            {
                Phase = TurnPhase.Locking;
            }
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

        private void FireCustomDiceFaceAbilities(GameState state, Player player)
        {
            // Snapshot active dice; effects may add/move dice
            var activeDice = Zones.Active.ToList();
            foreach (var die in activeDice)
            {
                if (!Zones.Active.Contains(die)) continue; // moved by a prior effect

                switch (die.DieType)
                {
                    case DieType.Artisan when die.IsStarFace:
                        ExecuteAdjustActive(state, player, "Artisan *: Choose a die to adjust");
                        break;

                    case DieType.Intrigue when die.IsDoubleStarFace:
                        ExecuteAdjustActive(state, player, "Intrigue **: Choose first die to adjust");
                        ExecuteAdjustActive(state, player, "Intrigue **: Choose second die to adjust");
                        break;

                    case DieType.Voyage:
                        ExecuteVoyageFace(state, player, die);
                        break;

                    case DieType.Decree when die.IsStarFace:
                        ExecuteDecreStar(state, player);
                        break;
                }
            }
        }

        private void ExecuteAdjustActive(GameState state, Player player, string prompt)
        {
            var candidates = Zones.Active.Where(d => d.HasPipValue).ToList();
            if (candidates.Count == 0) return;
            var target = player.Input.ChooseDie(candidates, prompt, player);
            if (target == null) return;
            int value = player.Input.ChoosePipValue(target, "Choose pip value", player);
            target.SetValue(value);
        }

        private void ExecuteVoyageFace(GameState state, Player player, Die voyageDie)
        {
            switch (voyageDie.Value)
            {
                case DieFaces.VoyageAdjust:
                    ExecuteAdjustActive(state, player, "Voyage *: Choose a die to adjust");
                    break;

                case DieFaces.VoyageReroll:
                {
                    var candidates = Zones.Active.Where(d => d != voyageDie).ToList();
                    var target = player.Input.ChooseDie(candidates, "Voyage R: Choose a die to reroll", player);
                    if (target != null) target.Roll(_rng);
                    break;
                }

                case DieFaces.VoyageDoubleDice:
                    for (int i = 0; i < 2; i++)
                    {
                        var tempDie = new Die(DieType.Standard) { IsTemporary = true };
                        tempDie.Roll(_rng);
                        player.DicePool.Add(tempDie);
                        Zones.Active.Add(tempDie);
                        Zones.Temporary.Add(tempDie);
                    }
                    break;

                case DieFaces.VoyageLock:
                {
                    var candidates = Zones.Active.Where(d => d != voyageDie && d.HasPipValue).ToList();
                    var target = player.Input.ChooseDie(candidates, "Voyage L: Choose a die to lock at any value", player);
                    if (target != null)
                    {
                        int value = player.Input.ChoosePipValue(target, "Choose pip value to lock at", player);
                        target.SetValue(value);
                        Zones.LockDie(target);
                    }
                    break;
                }
            }
        }

        private void ExecuteDecreStar(GameState state, Player player)
        {
            bool borrowTile = player.Input.ChooseYesNo(
                "Decree *: Borrow another player's tile ability? (No = adjust a die)", player);

            if (borrowTile)
            {
                var others = state.TurnOrder.Where(p => p != player && p.OwnedTiles.Count > 0).ToList();
                if (others.Count == 0)
                {
                    ExecuteAdjustActive(state, player, "Decree *: No tiles to borrow, adjust a die instead");
                    return;
                }

                var chosenPlayer = player.Input.ChoosePlayer(others, "Decree *: Choose a player to borrow from", player);
                if (chosenPlayer == null)
                {
                    ExecuteAdjustActive(state, player, "Decree *: Adjust a die instead");
                    return;
                }

                var tiles = chosenPlayer.OwnedTiles.ToList();
                var chosenTile = player.Input.ChooseTile(tiles, "Decree *: Choose a tile to borrow", player);
                if (chosenTile == null) return;

                // Execute the first activatable ability on the borrowed tile
                foreach (var ability in chosenTile.Abilities)
                {
                    if (ability.CanActivate(state, player))
                    {
                        ability.Execute(state, player);
                        break;
                    }
                }
            }
            else
            {
                ExecuteAdjustActive(state, player, "Decree *: Choose a die to adjust");
            }
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
