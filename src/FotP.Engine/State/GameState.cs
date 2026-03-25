using System;
using System.Collections.Generic;
using System.Linq;
using FotP.Engine.Core;
using FotP.Engine.Criteria;
using FotP.Engine.Dice;
using FotP.Engine.Market;
using FotP.Engine.Players;
using FotP.Engine.Tiles;

namespace FotP.Engine.State
{
    /// <summary>
    /// Central game state controller.
    /// </summary>
    public class GameState
    {
        public SmartList<Player> TurnOrder { get; } = new();
        public Player? CurrentPlayer { get; private set; }
        public int CurrentPlayerIndex { get; private set; }
        public GamePhase Phase { get; set; } = GamePhase.Setup;
        public MarketManager Market { get; } = new();
        public int RoundNumber { get; private set; }
        public Random Rng { get; }
        public TurnStateMachine TurnState { get; }

        /// <summary>Which side of the level bars is active for this game.</summary>
        public BarSide ActiveBarSide { get; private set; } = BarSide.A;

        /// <summary>Track the player who triggered the end-game (Queen claim).</summary>
        public Player? QueenClaimant { get; set; }

        /// <summary>In roll-off phase, tracks which players are still competing (clockwise from next after queen claimer).</summary>
        public List<Player> RollOffPlayers { get; } = new();

        /// <summary>The player currently holding the Pharaoh token (starts as QueenClaimant).</summary>
        public Player? PharaohHolder { get; set; }

        /// <summary>The pip score that must be matched or beaten to take the Pharaoh token.</summary>
        public int PharaohScore { get; set; }

        /// <summary>Optional roll-off score bar set by Royal Death artifact. Null means use standard pharaoh score.</summary>
        public int? RollOffBarScore { get; set; }

        public GameState(Random? rng = null)
        {
            Rng = rng ?? new Random();
            TurnState = new TurnStateMachine(Rng);
        }

        /// <summary>
        /// Setup a new game with given player names and inputs.
        /// </summary>
        public void Setup(List<(string name, IPlayerInput input)> playerConfigs, int startingDice = 3, BarSide barSide = BarSide.A)
        {
            foreach (var (name, input) in playerConfigs)
            {
                var player = new Player(name, input);
                // Give starting dice
                for (int i = 0; i < startingDice; i++)
                    player.DicePool.Add(new Die(DieType.Standard));

                TurnOrder.Add(player);
            }

            ActiveBarSide = barSide;
            SetupMarket();
            Phase = GamePhase.Playing;
            RoundNumber = 1;
            CurrentPlayerIndex = 0;
            CurrentPlayer = TurnOrder[0];
        }

        // Color composition per level: (yellowCount, blueCount, redCount).
        // Matches the current first-game bar layout; randomization selects which tiles fill each colour slot.
        private static readonly (int Y, int B, int R)[] LevelColorLayout =
        {
            (4, 0, 1), // level 3: 4 Yellow, 0 Blue, 1 Red
            (3, 2, 1), // level 4: 3 Yellow, 2 Blue, 1 Red
            (2, 1, 0), // level 5: 2 Yellow, 1 Blue
            (1, 1, 1), // level 6: 1 Yellow, 1 Blue, 1 Red
        };

        private void SetupMarket()
        {
            int playerCount = TurnOrder.Count;
            int stackSize = playerCount; // one copy per player per slot

            var pool = new TilePool();
            int slot = 0;

            // For each level 3-6: draw tiles randomly from TilePool by colour, shuffle, assign bar criteria.
            for (int i = 0; i < LevelColorLayout.Length; i++)
            {
                int level = i + 3;
                var (yCount, bCount, rCount) = LevelColorLayout[i];

                var levelTiles = new List<Tile>();
                for (int y = 0; y < yCount; y++)
                    levelTiles.Add(CreateFromDef(pool.DrawRandom(TileColor.Yellow, level, Rng)));
                for (int b = 0; b < bCount; b++)
                    levelTiles.Add(CreateFromDef(pool.DrawRandom(TileColor.Blue, level, Rng)));
                for (int r = 0; r < rCount; r++)
                    levelTiles.Add(CreateFromDef(pool.DrawRandom(TileColor.Red, level, Rng)));

                Shuffle(levelTiles);
                AssignCriteriaAndAddStacks(levelTiles, level, stackSize, ref slot);
            }

            // Level 7 – Queen (only 1 copy, always last, slot 0 of level-7 bar)
            var queenTile = TileFactory.CreateTile("Queen", 7, TileColor.Yellow);
            var queenBarConfig = LevelBarConfig.Get(7, ActiveBarSide);
            queenTile.ClaimCriteria = queenBarConfig.SlotCriteria[0];
            Market.Stacks.Add(new TileStack(queenTile, 1, slot));
        }

        private void AssignCriteriaAndAddStacks(
            List<Tile> tiles, int level, int stackSize, ref int slotIndex)
        {
            var barConfig = LevelBarConfig.Get(level, ActiveBarSide);
            for (int i = 0; i < tiles.Count; i++)
            {
                // If there are fewer bar slots than tiles (shouldn't happen), fall back to no criteria.
                if (i < barConfig.SlotCriteria.Count)
                    tiles[i].ClaimCriteria = barConfig.SlotCriteria[i];

                Market.Stacks.Add(new TileStack(tiles[i], stackSize, slotIndex++));
            }
        }

        private static Tile CreateFromDef(TileDefinition def)
            => TileFactory.CreateTile(def.Name, def.Level, def.Color);

        private void Shuffle<T>(List<T> list)
        {
            for (int i = list.Count - 1; i > 0; i--)
            {
                int j = Rng.Next(i + 1);
                (list[i], list[j]) = (list[j], list[i]);
            }
        }

        /// <summary>Start a turn for the current player.</summary>
        public void StartTurn()
        {
            if (CurrentPlayer == null)
                throw new InvalidOperationException("No current player.");
            TurnState.BeginTurn(CurrentPlayer, this);
        }

        /// <summary>Advance to the next player. Returns true if round is complete.</summary>
        public bool NextPlayer()
        {
            CurrentPlayerIndex = (CurrentPlayerIndex + 1) % TurnOrder.Count;
            CurrentPlayer = TurnOrder[CurrentPlayerIndex];

            if (CurrentPlayerIndex == 0)
            {
                RoundNumber++;
                return true; // Round complete
            }
            return false;
        }

        /// <summary>
        /// Enter roll-off phase after Queen is claimed.
        /// Records the queen claimer's pyramid score as the initial Pharaoh score,
        /// and builds the roll-off order clockwise from the next player after the claimer.
        /// </summary>
        public void EnterRollOff()
        {
            Phase = GamePhase.RollOff;

            PharaohHolder = QueenClaimant;
            PharaohScore = QueenClaimant?.PyramidScore ?? 0;

            RollOffPlayers.Clear();
            if (QueenClaimant == null) return;

            var ordered = TurnOrder.ToList();
            int queenIdx = ordered.IndexOf(QueenClaimant);
            int count = ordered.Count;

            // Clockwise from the next player after the queen claimer
            for (int i = 1; i < count; i++)
            {
                int idx = (queenIdx + i) % count;
                RollOffPlayers.Add(ordered[idx]);
            }
        }

        /// <summary>Determine the winner. Returns PharaohHolder if roll-off has been run; otherwise falls back to pyramid score.</summary>
        public Player DetermineWinner()
        {
            Phase = GamePhase.GameOver;

            if (PharaohHolder != null)
                return PharaohHolder;

            // Fallback for games without roll-off
            Player? winner = null;
            int bestScore = -1;
            foreach (var player in TurnOrder)
            {
                if (player.PyramidScore > bestScore)
                {
                    bestScore = player.PyramidScore;
                    winner = player;
                }
            }
            return winner ?? TurnOrder[0];
        }
    }
}
