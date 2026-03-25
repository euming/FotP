"""
Populate the FotP-GameLoop swarm-plan with action nodes that reference
the FotP-Rules-v20 DB objects via SmartList path annotations.

Each game loop node gets:
  - A description of what happens at this step
  - References to Rules DB paths for the full rule details
"""
import subprocess
import sys

PROJECT = "FotP-GameLoop"
BASE = "smartlist/execution-plan/fotp-gameloop/10-children"
RULES = "smartlist/execution-plan/fotp-rules-v20/10-children"


def insert(name, parent, desc):
    cmd = [
        "scripts\\ams.bat", "swarm-plan",
        "--project", PROJECT,
        "insert", name,
        "--parent", f"{BASE}/{parent}",
        "--actor-id", "claude-opus",
    ]
    if desc:
        cmd.extend(["--description", desc.strip()])
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    tag = "OK" if r.returncode == 0 else "ERR"
    print(f"  {tag}: {name}")
    if r.returncode != 0:
        print(f"       {r.stderr.strip()}", file=sys.stderr)
    return r.returncode == 0


def annotate(node_slug, parent_slug, title, text):
    """Add an observation/annotation to an existing node."""
    node_path = f"{BASE}/{parent_slug}/10-children/{node_slug}"
    cmd = [
        "scripts\\ams.bat", "swarm-plan",
        "--project", PROJECT,
        "annotate",
        "--node-path", node_path,
        "--title", title,
        "--text", text.strip(),
        "--actor-id", "claude-opus",
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        print(f"    WARN annotate: {r.stderr.strip()[:120]}", file=sys.stderr)


def ref(rules_path):
    """Format a reference to a Rules DB node."""
    return f"[ref:{RULES}/{rules_path}]"


print("=== Game Setup ===")
insert("Place Level Bars", "game-setup",
       f"Place 5 double-sided level bars (A/B side). {ref('level-bar-criteria-a-side')}")
insert("Stack Tiles on Bars", "game-setup",
       f"Stack tiles on bar slots. Tiles per stack = num_players - 1. {ref('game-setup/10-children/tile-stacks')}")
insert("Place Pharaoh on Queen", "game-setup",
       f"Place Pharaoh token on Queen tile (level 7). {ref('level-7-tiles/10-children/queen-yellow-level-7')}")
insert("Distribute Starting Gear", "game-setup",
       f"Each player: 1 pyramid, 3 standard dice, 1 reroll token. {ref('game-setup/10-children/player-setup')}")
insert("Determine Start Player", "game-setup",
       "Youngest player gets Start Player tile, goes first. Clockwise order.")

print("\n=== Round Loop ===")
insert("Cycle Through Players", "round-loop",
       "Each player takes a turn in clockwise order from start player. Repeat rounds.")
insert("Check Queen Claimed", "round-loop",
       f"After Queen claim, finish current round then enter Final Roll-Off. {ref('final-roll-off')}")

print("\n=== Player Turn ===")
insert("Set Current Player", "player-turn",
       "GameState.currentPlayer = next player. Code: GameState.StartTurn().")
insert("Execute Turn Phases", "player-turn",
       f"StartOfTurn -> RollLoop -> ClaimPhase -> PostClaim -> EndOfTurn. {ref('player-turn-structure')}")
insert("Handle Extra Turn", "player-turn",
       f"Omen/Good Omen/Queen's Favor -> player takes another turn. {ref('tile-power-categories/10-children/extra-turn-powers')}")

print("\n=== Start of Turn Phase ===")
insert("Add Bonus Dice to Cup", "start-of-turn-phase",
       f"Add dice from owned tiles. {ref('tile-power-categories/10-children/roll-extra-dice-powers')}")
insert("Fire StartOfTurn Triggers", "start-of-turn-phase",
       f"TileAbility.Trigger.StartOfTurn. {ref('player-turn-structure/10-children/start-of-turn')}")
insert("Reset Per-Turn State", "start-of-turn-phase",
       "Clear isUsedThisTurn on all abilities. Reset diceLockedThisTurn = 0. Remove old temp dice.")

print("\n=== Roll Loop ===")
insert("Roll All Dice in Cup", "roll-loop",
       "Physics roll all active dice. Code: DiceCup.OnMouseDown() -> PharoahDie.RollDie().")
insert("Enter Lock Phase", "roll-loop",
       f"Must lock >= 1 die before other actions. {ref('player-turn-structure/10-children/lock-phase')}")
insert("Optional Scarab Phase", "roll-loop",
       f"Spend tokens on active dice. {ref('scarab-token-system')}")
insert("Continue or Stop", "roll-loop",
       f"Roll again or proceed to claim. {ref('player-turn-structure/10-children/continue-or-stop-decision')}")

print("\n=== Roll Phase ===")
insert("Roll Dice Physics", "roll-phase",
       "PharoahDie.RollDiePhysics(). Wall colliders contain dice. Wait for Rigidbody.IsSleeping().")
insert("Read Die Values", "roll-phase",
       f"Read face-up values. Custom dice trigger abilities. {ref('custom-dice-special-face-abilities')}")
insert("Fire AfterRoll Triggers", "roll-phase",
       f"Estate Overseer/Granary Master increment stored die. {ref('tile-power-categories/10-children/stored-die-powers-increment-per-roll')}")
insert("Track Roll Number", "roll-phase",
       "Some abilities are first-roll-only: Farmer, Worker, Soldier, Soothsayer, Artisan (1st roll pip).")

print("\n=== Lock Phase ===")
insert("Player Selects Die to Lock", "lock-phase",
       "Click active die -> PharoahDie.MoveToLockedArea(). Must lock >= 1 per roll.")
insert("Auto-Lock Immediate Dice", "lock-phase",
       f"White dice must lock immediately. {ref('dice-types-and-faces/10-children/immediate-die-white')}")
insert("Fire LockedAny Triggers", "lock-phase",
       f"Herder: locked pair -> +1 die. {ref('player-turn-structure/10-children/lock-phase')}")
insert("Validate Lock Legality", "lock-phase",
       "Cannot unlock previously-locked die. Cannot roll without locking new die.")

print("\n=== Scarab Phase ===")
insert("Spend Reroll Token", "scarab-phase",
       f"Reroll 1 active die. {ref('scarab-token-system/10-children/reroll-token')}")
insert("Spend Pip Token", "scarab-phase",
       f"Add +1 pip, max die value. {ref('scarab-token-system/10-children/1-pip-token')}")
insert("Spend Die Token", "scarab-phase",
       f"Add 1 temp standard die, roll immediately. {ref('scarab-token-system/10-children/1-die-token')}")
insert("Activate Tile Powers", "scarab-phase",
       f"Use active tile abilities: adjust, reroll, lock. {ref('tile-power-categories/10-children/adjust-die-powers')}")

print("\n=== Continue or Stop Decision ===")
insert("Roll Again", "continue-or-stop-decision",
       "Active dice in cup -> return to Roll Phase. PlayerGameState.mayRollDice = true.")
insert("All Locked - Must Stop", "continue-or-stop-decision",
       f"No active dice -> proceed to Claim. Fire AllLocked triggers. {ref('player-turn-structure/10-children/continue-or-stop-decision')}")
insert("Fire AllLocked Triggers", "continue-or-stop-decision",
       f"Priest of the Dead, Spirit of the Dead, Burial Mask: +1 die at any value. "
       f"{ref('tile-power-categories/10-children/reroll-powers')}")
insert("Player Stops Early", "continue-or-stop-decision",
       "Voluntary stop with active dice remaining. DiceCup right-click or End Turn.")

print("\n=== Claim Phase ===")
insert("Evaluate Criteria", "claim-phase",
       f"Check locked dice vs level bars. {ref('tile-claiming-rules/10-children/criteria-matching')}")
insert("Display Claimable Tiles", "claim-phase",
       f"Highlight valid tiles. Gray out unavailable. {ref('tile-claiming-rules/10-children/claiming-restrictions')}")
insert("Claim One Tile", "claim-phase",
       "PlayerBoard.ClaimTile() -> Instantiate(tile) -> add to tileList.")
insert("Pass - Gain 2 Tokens", "claim-phase",
       f"No claim -> 2 scarab tokens. {ref('scarab-token-system/10-children/gaining-tokens')}")
insert("Fire Acquire Triggers", "claim-phase",
       "TileAbility.Trigger.Acquire. Some artifacts trigger immediately on acquire.")

print("\n=== Post-Claim Effects ===")
insert("Extra Turn - Omen", "post-claim-effects",
       f"Take another turn, -1 standard die. NOT IMPLEMENTED. {ref('level-3-tiles/10-children/omen-red-level-3')}")
insert("Extra Turn - Good Omen", "post-claim-effects",
       f"Take another turn, normal dice. NOT IMPLEMENTED. {ref('level-4-tiles/10-children/good-omen-red-level-4')}")
insert("Extra Turn - Queen's Favor", "post-claim-effects",
       f"Claim tile + extra turn. NOT IMPLEMENTED. {ref('level-7-tiles/10-children/queen-s-favor-red-level-7')}")
insert("Extra Claims - Secret Passage", "post-claim-effects",
       f"Claim up to 2 level 3 tiles. NOT IMPLEMENTED. {ref('level-6-tiles/10-children/secret-passage-red-level-6')}")
insert("Extra Claims - Treasure", "post-claim-effects",
       f"Split dice, claim 2 tiles. NOT IMPLEMENTED. {ref('level-6-tiles/10-children/treasure-red-level-6')}")
insert("Extra Claims - Royal Power", "post-claim-effects",
       f"Claim up to 2 blue tiles. NOT IMPLEMENTED. {ref('level-7-tiles/10-children/royal-power-red-level-7')}")
insert("Queen Claim - Trigger Roll-Off", "post-claim-effects",
       f"Take Pharaoh, announce score, trigger roll-off. NOT IMPLEMENTED. {ref('final-roll-off/10-children/roll-off-trigger')}")

print("\n=== End of Turn Phase ===")
insert("Return Dice to Cup", "end-of-turn-phase",
       "PharoahDie.ReadyToRoll() for each die. Move to InCupArea.")
insert("Remove Temporary Dice", "end-of-turn-phase",
       "Destroy temp dice (bIsTemporary flag). From +1 Die tokens, Charioteer, etc.")
insert("Reset Ability Flags", "end-of-turn-phase",
       "Clear isUsedThisTurn, isUsedThisRoll. Reset per-roll artifact states.")
insert("Fire EndOfTurn Triggers", "end-of-turn-phase",
       "TileAbility.Trigger.EndOfTurn. Bad Omen affects other players here.")
insert("Advance to Next Player", "end-of-turn-phase",
       "Next player clockwise. If round complete + Queen claimed -> Final Roll-Off.")

print("\n=== Final Roll-Off ===")
insert("Pharaoh Token Transfer", "final-roll-off",
       f"Queen claimer takes token, announces score. NOT IMPLEMENTED. {ref('final-roll-off/10-children/roll-off-trigger')}")
insert("Complete Current Round", "final-roll-off",
       f"Remaining players get normal turns. NOT IMPLEMENTED. {ref('final-roll-off/10-children/round-completion')}")
insert("Compensate Skipped Players", "final-roll-off",
       f"Players who missed a turn get +1 die token. NOT IMPLEMENTED. {ref('final-roll-off/10-children/roll-off-trigger')}")
insert("Roll-Off Turns", "final-roll-off",
       f"Each player gets 1 final turn. Beat Pharaoh score to claim token. "
       f"NOT IMPLEMENTED. {ref('final-roll-off/10-children/roll-off-turns')}")
insert("Queen's Last Chance", "final-roll-off",
       f"Queen claimer's final attempt if Pharaoh was taken. NOT IMPLEMENTED. {ref('final-roll-off/10-children/queen-s-last-chance')}")
insert("Determine Winner", "final-roll-off",
       f"Pharaoh holder wins. Tie-break: right of Queen claimer. NOT IMPLEMENTED. {ref('final-roll-off/10-children/determining-the-winner')}")

print("\n=== Victory ===")
insert("Display Winner", "victory",
       "Show winning player, final score, tiles collected. NOT IMPLEMENTED.")
insert("Game Over Options", "victory",
       "Rematch, menu, etc. NOT IMPLEMENTED.")

print("\n=== Done ===")
