using UnityEngine;
using System.Collections;
/*
 * PlayerGameState - keeps track of when the player can roll, must lock dice, or pass the turn.
 */
public class PlayerGameState : MonoBehaviour {

	public enum PlayerGameStates {
		Uninitialized = -1,
		InitTurn,			//	initialization stuff at start of turn

		//	loop
		ReadyToRollDice,	//	waiting for player to roll the dice
		DiceHaveBeenRolled,	//	dice have been rolled. Player may choose some actions
		WaitingForLock,		//	waiting for player to lock at least one die

		//	end loop
		WaitingForPurchaseTile,	//	player may choose a tile to purchase
		TilePurchaseChosen,		//	player has chosen a tile to purchase. Waiting for final confirmation
		EndTurn,			//	my turn is officially done. 
		WaitingNextTurn,	//	waiting for another player's turn to be done so I can go
	};

	public PlayerGameStates curState;
	public bool				isInitialRoll = true;
	public bool				mayRollDice = false;
	public int				diceLockedThisTurn;
	int						lastDiceLockedThisTurn;

	void Awake() {
		curState = PlayerGameStates.WaitingNextTurn;
		mayRollDice = false;
		diceLockedThisTurn = 0;
	}
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		if (curState == PlayerGameStates.WaitingForLock) {
			if (diceLockedThisTurn > 0) {
				mayRollDice = true;
			} else {
				mayRollDice = false;
			}
			if (lastDiceLockedThisTurn != diceLockedThisTurn) {
				if (diceLockedThisTurn > 0) {
					GameState.Message(this.name + " has " + diceLockedThisTurn.ToString() + " locked dice and may roll dice.");
				}
				else {
					GameState.Message(this.name + " has " + diceLockedThisTurn.ToString() + " locked dice and must lock a die.");
				}
				lastDiceLockedThisTurn = diceLockedThisTurn;
			}
		}
	}

	public void InitTurn()
	{
		diceLockedThisTurn = 0;
		mayRollDice = true;
		isInitialRoll = true;
	}

	//	set some stuff up when we enter these states
	public void SetState(PlayerGameStates newState)
	{
		curState = newState;
		switch (curState) {
		case PlayerGameStates.InitTurn:
			InitTurn();
			SetState(PlayerGameStates.ReadyToRollDice);
			break;
		case PlayerGameStates.ReadyToRollDice:
			mayRollDice = true;
			break;
		case PlayerGameStates.DiceHaveBeenRolled:
			mayRollDice = false;
			isInitialRoll = false;
			break;
		case PlayerGameStates.WaitingForLock:
			diceLockedThisTurn = 0;
			lastDiceLockedThisTurn = 0;
			mayRollDice = false;
			break;
		case PlayerGameStates.WaitingForPurchaseTile:
			mayRollDice = false;
			break;
		case PlayerGameStates.TilePurchaseChosen:
			mayRollDice = false;
			break;
		case PlayerGameStates.WaitingNextTurn:
			mayRollDice = false;
			break;
		}
	}

	public void StartTurn()
	{
		if (curState != PlayerGameStates.WaitingNextTurn)
			return;
		SetState (PlayerGameStates.InitTurn);
	}
}
