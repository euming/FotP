using UnityEngine;
using System.Collections;
/*
 * PlayerGameState - keeps track of when the player can roll, must lock dice, or pass the turn.
 */
public class PlayerGameState : MonoBehaviour {

    public enum PlayerGameStates {
        Uninitialized = -1,
        InitTurn,           //	initialization stuff at start of turn

        //	loop
        ReadyToRollDice,    //	waiting for player to roll the dice
        DiceHaveBeenRolled, //	dice have been rolled. Player may choose some actions
        WaitingForLock,     //	waiting for player to lock at least one die

        //	end loop
        AllDiceLocked,          //  all the dice have been locked
        WaitingForPurchaseTile, //	player may choose a tile to purchase
        TilePurchaseChosen,     //	player has chosen a tile to purchase. Waiting for final confirmation
        EndTurn,            //	my turn is officially done. 
        WaitingNextTurn,    //	waiting for another player's turn to be done so I can go

        //  outside of game loop states
        WaitingToSelectDie, //  if some tile/scarab ability requires me to select a die, I will be in this state
    };

    //  delegate is a C# safe function pointer.
    public delegate bool delOnDieSelect(PharoahDie die);
    //  do something when a die is selected. We don't know what. That is for the ability to decide.
    public delOnDieSelect   OnDieSelect;
    //  do something when the player selects done
    public delOnDieSelect   OnDieDone;
    //  do something when the player selects cancel
    public delOnDieSelect   OnDieCancel;

    public PlayerGameStates lastState;          //  for undoing
    public PlayerGameStates curState;
    public bool isInitialRoll = true;
    public bool mayRollDice = false;
    public bool mayPurchaseTile = false;
    public int diceLockedThisTurn;
    int lastDiceLockedThisTurn;
    PlayerBoard myPlayer;
    PharoahDie curDie;

    void Awake() {
        curState = PlayerGameStates.WaitingNextTurn;
        mayRollDice = false;
        diceLockedThisTurn = 0;
        myPlayer = GetComponent<PlayerBoard>();
    }
    // Use this for initialization
    void Start() {

    }

    // Update is called once per frame
    void Update() {
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
        mayPurchaseTile = false;
        myPlayer.FireTriggers(TileAbility.PlayerTurnStateTriggers.StartOfTurn);
    }

    //  go back to the previous state
    public PlayerGameStates UndoState()
    {
        SetState(lastState);
        return curState;
    }

	//	set some stuff up when we enter these states
	public void SetState(PlayerGameStates newState)
	{
        lastState = curState;
        curState = newState;
		switch (curState) {
		case PlayerGameStates.InitTurn:
			InitTurn();
			SetState(PlayerGameStates.ReadyToRollDice);
			break;
		case PlayerGameStates.ReadyToRollDice:
			mayRollDice = true;
            mayPurchaseTile = false;
            break;
		case PlayerGameStates.DiceHaveBeenRolled:
			mayRollDice = false;
			isInitialRoll = false;
            break;
		case PlayerGameStates.WaitingForLock:
			diceLockedThisTurn = 0;
			lastDiceLockedThisTurn = 0;
			mayRollDice = false;
            mayPurchaseTile = true;
            break;
		case PlayerGameStates.WaitingForPurchaseTile:
			mayRollDice = false;
            break;
		case PlayerGameStates.TilePurchaseChosen:
			mayRollDice = false;
            mayPurchaseTile = false;
            break;
		case PlayerGameStates.WaitingNextTurn:
			mayRollDice = false;
            mayPurchaseTile = false;
            break;
		}
	}

	public void StartTurn()
	{
		if (curState != PlayerGameStates.WaitingNextTurn)
			return;
		SetState (PlayerGameStates.InitTurn);
	}

    //  selecting a die
    public bool ChooseDie(PharoahDie die)
    {
        bool bSuccess = false;
        if (OnDieSelect != null)
        {
            bSuccess = OnDieSelect(die);
            if (bSuccess)
             curDie = die;
        }
        return bSuccess;
    }

    public void OnDoneClick()
    {
        this.OnDieDone(curDie);
    }
    public void OnCancelClick()
    {
        this.OnDieCancel(curDie);

    }
}
