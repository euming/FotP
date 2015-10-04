using UnityEngine;
using System.Collections;
using System.Collections.Generic;

//	a tile is one of things you buy that gives you abilties to help you win the game
public class Tile : SelectableObject {
    public enum TestingState
    {
        NotImplemented,
        PartiallyImplemented,
        Completed_ReadyToTest,
        Completed_HasBug,
        Completed_PassedTests,
    };

    public TestingState testingState;

    public string       hintText;   //  official hint text. same as in the rule book.
	public BarSlot		mySlot;		//	where I belong before I'm bought. I may have to go back here if the player changes his mind before hitting DONE.
	public int		shopRow;	//	index of shop row
	public List<int> shopCol;	//	index of valid shop columns
    public bool canUndo;        //  if purchased, can we undo this purchase this turn?

	//	figure out which slot I should be in, and put myself there.
	void AutoAssignToSlot()
	{
		GameState		gs = GameState.GetCurrentGameState();
		TileShop 		ts = gs.tileShop;

		//	am I already in a valid slot? if so, bail
		if (mySlot != null) {	//	we already are in a slot
			//	is this the slot that we are supposed to be in anyway?
			if (isValidSlot(mySlot)) {
				//	if so, then don't do anything else because we have already successfully "autoassigned" to this slot
				return;
			}
			mySlot.removeChild(this.gameObject);	//	remove this from the slot
		}

		//	first get the bar according to the shop Row index
		Bar bar = ts.barList[shopRow];
		foreach(int colIdx in shopCol) {
			BarSlot slot = bar.barSlotList[colIdx];
			if (slot.isEmpty()) {
				slot.addChild(this.gameObject);
                break;	//	we're done
			}
		}
	}


	// Use this for initialization
	void Start () {
        canUndo = true;
        AutoAssignToSlot();
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	//	see if this slot is one of the valid slots we have designated in the Unity data properties.
	public bool isValidSlot(BarSlot testSlot)
	{
		GameState		gs = GameState.GetCurrentGameState();
		TileShop 		ts = gs.tileShop;

		Bar bar = ts.barList[shopRow];
		foreach(int colIdx in shopCol) {
			BarSlot slot = bar.barSlotList[colIdx];
			if (slot == testSlot) {
				return true;
			}
		}
		return false;
	}

	//	go back to where I came from
	void ReturnToSlot()
	{
		mySlot.ReturnOne();
	}

	public void OnAcquire(PlayerBoard plr)
	{
		TileAbility[] abilities = GetComponents<TileAbility>();
		foreach(TileAbility ability in abilities) {
			ability.OnAcquire(plr);
		}
	}
	public void OnAcquireUndo(PlayerBoard plr)
	{
		TileAbility[] abilities = GetComponents<TileAbility>();
		foreach(TileAbility ability in abilities) {
			ability.OnAcquireUndo(plr);
		}
	}

	public override void OnSelect(PlayerBoard currentPlayer) {
		base.OnSelect(currentPlayer);
		if (currentPlayer.Has(this)) {
            if (this.canUndo)
            {
                this.ReturnToSlot();
                currentPlayer.Drop(this);
            }
            else if (this.canActivate())
            {
                this.FireTrigger(TileAbility.PlayerTurnStateTriggers.Select, currentPlayer);
            }
        }
		else {
			bool bQualifiedToPurchase = false;
			if (mySlot) 
				bQualifiedToPurchase = mySlot.isQualified();
			else
				Debug.LogError("No Slot found for Tile " + this.name);
			if (bQualifiedToPurchase) {
				bool bGotOne = mySlot.HasOne();
				if (bGotOne) {
					bool bSuccessfulTake = currentPlayer.Take(this);
                    if (bSuccessfulTake)
                        mySlot.TakeOne();
				}
				else {
					GameState.Message(mySlot.name + " is out of " + this.name + " so " + currentPlayer.name + " got none!");
				}
			}
			else {
				string msg = (mySlot.name + " is not qualified to buy " + this.name + " because it didn't satisfy " + mySlot.name);
				GameState.Message(msg);
			}
		}
	}

    public bool canActivate()
    {
        bool bCan = false;
        if (this.GetComponent<TileAbility>() != null)
            bCan = true;
        return bCan;
    }

    //  debug stuff
    public string GetDebugStatusString()
    {
        string statusStr = "";
        
        switch (testingState)
        {
            default:
            case TestingState.NotImplemented:
                statusStr = "?    \t";
                break;
            case TestingState.PartiallyImplemented:
                statusStr = "pp   \t";
                break;
            case TestingState.Completed_ReadyToTest:
                statusStr = "RRR  \t";
                break;
            case TestingState.Completed_HasBug:
                statusStr = "BBBB \t";
                break;
            case TestingState.Completed_PassedTests:
                statusStr = "PPPPP\t";
                break;
        }
        return statusStr;
    }

    //  *********************   TileAbility stuff
    public void FireTrigger(TileAbility.PlayerTurnStateTriggers trigState, PlayerBoard plr)
    {
        TileAbility[] abilityList = GetComponents<TileAbility>();
        foreach(TileAbility ability in abilityList)
        {
            ability.FireTrigger(trigState, plr);
        }
    }
}
