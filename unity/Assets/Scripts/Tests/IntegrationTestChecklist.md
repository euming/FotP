# FotP Unity Integration Test Checklist

Manual test steps to verify a complete game can be played in Unity Editor Play Mode.

## Prerequisites

- Unity Editor open with the FotP project
- TextMeshPro package installed (Window > Package Manager)
- All scenes added to Build Settings:
  - MainMenu
  - Game
  - Results

---

## 1. Main Menu → Game Setup

1. Open `Assets/Scenes/MainMenu.unity`
2. Enter Play Mode (▶)
3. **Verify:** Main Menu canvas is visible with Play/Quit buttons and player count selector
4. Click **"2 Players"** (or leave default)
5. Click **Play**
6. **Verify:** Game scene loads and `GameController` logs in Console:
   ```
   [GameController] GameState initialized. Phase=Setup, Players=2
   ```

---

## 2. Dice Rolling — Human Turn

1. In the Game scene, the first player's turn begins
2. **Verify:** DiceSelectionPanel appears showing available dice
3. Select dice to roll (click to toggle lock)
4. **Verify:** ContinueRollingPanel shows after roll with option to keep rolling or stop
5. Roll at least once more
6. Click **"Stop Rolling"** when satisfied
7. **Verify:** Turn advances to next player

---

## 3. Tile Claiming

1. After a player finishes rolling, the TileSelectionPanel should appear
2. **Verify:** Available tiles matching the rolled pips are highlighted/selectable
3. Select a tile to claim
4. **Verify:** Tile is added to the player's owned tiles (check Console log or HUD)
5. **Verify:** The turn advances to the next player

---

## 4. Scarab Spending

1. During a roll phase, if the player has scarabs (red dice showing scarab face):
2. **Verify:** ScarabSelectionPanel appears, listing available scarab options
3. Select a scarab option (e.g., re-roll, convert pip)
4. **Verify:** Scarab is consumed and the action takes effect
5. Continue turn normally

---

## 5. Tile Abilities

1. Some tiles have special abilities that trigger on claim or on use
2. When a tile ability triggers, **verify:** a prompt panel appears (YesNoPanel or similar)
3. Respond to the prompt (Yes/No)
4. **Verify:** The ability effect is applied (tile moved, extra action granted, etc.)
5. **Verify:** No console errors thrown during ability resolution

---

## 6. Multiple Rounds

1. Play through at least 3 full rounds (all players take a turn each round)
2. **Verify:** Turn order cycles correctly (Player 1 → Player 2 → Player 1 → ...)
3. **Verify:** No null reference exceptions in Console
4. **Verify:** Score/tile counts update visibly between turns

---

## 7. AI Player Turn (if configured)

1. If Player 2 is configured as AI (set in GameController Inspector):
2. **Verify:** AI turn executes automatically without user input
3. **Verify:** AI eventually ends its turn and control returns to human player
4. **Verify:** AI-claimed tiles appear in game state

---

## 8. Game Over and Results Scene

1. Play until tiles run out or the game engine signals game over
   - Alternatively, in the GameController Inspector, temporarily reduce tile count to speed this up
2. **Verify:** `OnGameOver` event fires (check Console: `[GameController] Game over`)
3. **Verify:** Results scene loads automatically
4. **Verify:** Results canvas shows:
   - Winner name in large text ("Player X Wins!")
   - Score summary for all players (PyramidScore, tile count)
5. Click **Rematch**
6. **Verify:** Game scene reloads and a new game starts
7. From Results, click **Main Menu**
8. **Verify:** Main Menu scene loads correctly

---

## 9. Roll-Off Tiebreaker

1. Arrange a tie (equal PyramidScore for two players — may require Debug.Log inspection)
2. **Verify:** `DetermineWinner()` triggers a roll-off in the engine
3. **Verify:** The roll-off winner is displayed in the Results scene (not "tie")

---

## Pass Criteria

- [ ] Main Menu loads and player count is configurable
- [ ] Game scene initialises with correct player count
- [ ] Human player can roll, lock dice, and stop
- [ ] Tiles can be claimed after rolling
- [ ] Scarab spending prompts appear and work
- [ ] Tile ability prompts appear and resolve without errors
- [ ] AI player (if present) takes its turn automatically
- [ ] Game ends when tiles are exhausted
- [ ] Results scene shows winner and scores
- [ ] Rematch and Main Menu navigation work
- [ ] No unhandled exceptions in Console throughout the session
