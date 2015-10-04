using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[System.Serializable]

public class PlayerBoard : MonoBehaviour {

	public List<Tile>	tileList;	//	all my tiles
	public List<PharoahDie>	diceList;	//	all my dice
	public List<Scarab>		scarabList;		//	all my scarabs

    Tile curTileInUse;              //  while we're using a tile, keep track of it.
    Scarab curScarabInUse;          //  while we're using the scarab, keep it separate from the others
    PlayerGameState pgs;
    bool isFirstRoll = false;
    

	public void NewGame()
	{
        curTileInUse = null;
        curScarabInUse = null;
        tileList.Clear();
		diceList.Clear();
		for (int ii=0; ii<3; ++ii) {
			Die d6 = AddDie(DiceFactory.DieType.Red);
			d6.transform.parent = this.transform;
		}
		HideDice ();
		this.gameObject.SetActive (false);
	}

	void Awake() {
		pgs = GetComponent<PlayerGameState> ();
	}
	// Use this for initialization
	void Start () {
	}
	
	// Update is called once per frame
	void Update () {
	}

	//	========================================================================================
	//	scarab stuff
	public Scarab AddScarab(Scarab.ScarabType scarabType)
	{
        GameObject bugGO = GameObject.Instantiate(GameState.GetCurrentGameState().scarabPrefab.gameObject);
        Scarab bug = bugGO.GetComponent<Scarab>();
        bug.SetType(scarabType);
		scarabList.Add(bug);
        bugGO.transform.parent = this.transform;    //  put this under the player board hierarchy.
		return bug;
	}
	public void DestroyScarab(Scarab scarab)
	{
		scarabList.Remove(scarab);
		Destroy (scarab.gameObject);
	}

    public bool hasScarab(Scarab.ScarabType type)
    {
        bool hasIt = false;
        foreach (Scarab sc in scarabList) {
            if (sc.type == type)
            {
                hasIt = true;
                break;
            }
        }
            return hasIt;
    }
    public Scarab PopScarab(Scarab.ScarabType type)
    {
        Scarab hasIt = null;
        foreach (Scarab sc in scarabList)
        {
            if (sc.type == type)
            {
                hasIt = sc;
                break;
            }
        }
        if (hasIt != null)
            scarabList.Remove(hasIt);
        return hasIt;
    }
    public bool UseScarab(Scarab.ScarabType type)
    {
        bool bSuccess = false;
        bool bHasScarab = hasScarab(type);
        if (bHasScarab) {
            curScarabInUse = PopScarab(type);
            //  this will wait until the player has selected a die, and then perform the scarab's delegate function on that die.
            this.AskToChooseDie(curScarabInUse.onDieSelect, type.ToString());
        }

        return bSuccess;
    }
    //  =============================================================================

    //	add a new die to myself
    public PharoahDie AddDie(DiceFactory.DieType dieType)
	{
		PharoahDie die = GameState.GetCurrentGameState().diceFactory.NewDie(dieType);
		diceList.Add(die);
		die.ReadyToRoll();
		return die;
	}

	//	========================================================================================
	//	dice stuff

    public int GetNumValidDice(TileAbility.DieType onlyDieType)
    {
        int nDice = 0;
        foreach(PharoahDie die in diceList)
        {
            if (die.isDieType(onlyDieType))
            {
                nDice++;
            }
        }
        return nDice;
    }
    public void DestroyDie(PharoahDie die)
	{
		die.ReadyToRoll();
		diceList.Remove(die);
		Destroy (die.gameObject);
	}

	//	do we own a tile?
	public bool Has(Tile tile)
	{
		foreach(Tile t in tileList) {
			if (t==tile) return true;
		}
		return false;
	}

    public bool Take(Tile newTile)
    {
        bool bSuccess = false;
        if (this.pgs.mayPurchaseTile) {
            GameState.Message(this.name + " takes " + newTile.name);
            tileList.Add(newTile);
            newTile.FireTrigger(TileAbility.PlayerTurnStateTriggers.Acquire, this);
            TilePurchaseChosen();
            bSuccess = true;
        }
        else
        {
            GameState.Message(this.name + " may not take " + newTile.name);
        }
		return bSuccess;
	}

	public bool Drop(Tile tile)
	{
		bool bSuccess = false;
        if (Has(tile)) {
            if (tile.canUndo) { 
                bSuccess = true;
                GameState.Message(this.name + " returns " + tile.name);
                tileList.Remove(tile);
                tile.FireTrigger(TileAbility.PlayerTurnStateTriggers.AcquireUndo, this);
                UndoState();
            }
            else {

                {
                    GameState.Message(this.name + " already owns " + tile.name + " and cannot buy another.");
                }
            }
		}
		return bSuccess;
	}

	public void SortDiceList()
	{
		diceList.Sort();
	}

	public bool PlayerMayRollDice()
	{
		bool bMayRoll = false;
		//PlayerGameState pgs = GetComponent<PlayerGameState> ();
		if (pgs != null) {
			bMayRoll = pgs.mayRollDice;
		}
		return bMayRoll;
	}

    //  special select die state
    public bool isSelectingDie()
    {
        bool bIs = false;
        if (GetPlayerGameState() == PlayerGameState.PlayerGameStates.WaitingToSelectDie)
        {
            bIs = true;
        }
        return bIs;
    }

    //  the UI is asking the player to choose a die for some reason
    public void AskToChooseDie(PlayerGameState.delOnDieSelect del, string reason)
    {
        GameState.Message(this.name + " please choose a die for " + reason);
        pgs.SetState(PlayerGameState.PlayerGameStates.WaitingToSelectDie);
        pgs.OnDieSelect = del;  //  set the delegate
    }
    public void AskToChooseDone(PlayerGameState.delOnDieSelect del)
    {
        pgs.OnDieDone = del;
        UIState.EnableDoneButton();
    }
    public void AskToChooseCancel(PlayerGameState.delOnDieSelect del)
    {
        pgs.OnDieCancel = del;
        UIState.EnableCancelButton();
    }

    public void SetTileInUse(Tile tile)
    {
        curTileInUse = tile;
    }
    //  player has chosen a die
    public void ChooseDie(PharoahDie die)
    {
        pgs.UndoState();    //  go back to previous state before WaitingToSelectDie
        pgs.ChooseDie(die); //  calls OnDieSelect delegate. For scarabs, this will reroll or addpip. For TileAbility, it will call the ability's delegate, if any

        //  this should be made generic for all TileAbility
        if (this.curScarabInUse && this.curScarabInUse.isConsumed)
        {
            Destroy(this.curScarabInUse.gameObject);    //  destroy the scarab after we've rolled/added pip to the die.
            this.curScarabInUse = null;
        }

        //  This will fire the trigger from the player's point of view. The die chosen should be saved by the delegate in ChooseDie
        if (this.curTileInUse)
        {
            this.curTileInUse.FireTrigger(TileAbility.PlayerTurnStateTriggers.ChooseDie, this);
        }
    }

    //  player state stuff
    public PlayerGameState.PlayerGameStates GetPlayerGameState()
	{
		PlayerGameState.PlayerGameStates pgse = PlayerGameState.PlayerGameStates.Uninitialized;
		//PlayerGameState pgs = GetComponent<PlayerGameState> ();
		if (pgs != null) {
			pgse = pgs.curState;
		}
		return pgse;
	}

	//	how many dice can we roll? If it's 0, then we need to force an end to this turn.
	public int CountActiveDice()
	{
		int sum = 0;
		foreach(PharoahDie d6 in diceList) {
			if (d6.isInActiveArea() || (d6.isInNoArea())) {
				sum++;
			}
		}
		return sum;
	}

	//	return - were dice rolled or not?
	bool bForcePass = false;
	public bool RollDice()
	{
		if (!PlayerMayRollDice ()) {
			//	we can't roll dice because we just purchased a new tile. We can only end the turn here.
			if (pgs.curState == PlayerGameState.PlayerGameStates.TilePurchaseChosen) {
				GameState.EndTurn();
				return false;
			}
			GameState.Message(this.name + " is in state " + GetPlayerGameState().ToString() + "\nand cannot roll. Click again to pass turn.");
			if (bForcePass) {
				GameState.EndTurn();
			}
			bForcePass = true;
			return false;
		}   //  end player may not roll dice

        //  able to roll dice, continue
		bForcePass = false;
		int ndicerolled = 0;
		GameState.GetCurrentGameState ().purchaseBoard.SetState (PurchaseBoard.PurchaseBoardState.isTuckedAway);
		UnhideDice ();

        //  white dice must be locked automatically
        if (!isFirstRoll)
        {
            LockWhiteDice();
        }
		foreach(PharoahDie d6 in diceList) {
			d6.EndRoll();
			if (d6.isInActiveArea() || (d6.isInNoArea())) {
				d6.ReadyToRoll();
				if (pgs.isInitialRoll && d6.isSetDie()) {
					d6.MakeSetDie(d6.setDieValue);
				}
				else {
					d6.RollDie();
				}
				ndicerolled++;
			}
            isFirstRoll = false;
        }
		SortDiceList();
		GameState.Message (this.name + " rolling (" + ndicerolled.ToString() +"/"+diceList.Count.ToString() + ") dice");
		if (ndicerolled > 0) {
			pgs.SetState (PlayerGameState.PlayerGameStates.DiceHaveBeenRolled);
			return true;
		}
		GameState.WaitForPurchase ();
		return false;
	}

	//	put dice that have just been rolled into the active area so that it doesn't touch the purchase board.
	public void CollectLooseDice()
	{
		foreach(PharoahDie d6 in diceList) {
			if (d6.isInNoArea()) {
				d6.MoveToUnlockedArea();
			}
		}
	}

	public void LockedDieThisTurn()
	{
		pgs.diceLockedThisTurn++;
	}
	public void UnlockedDieThisTurn()
	{
		pgs.diceLockedThisTurn--;
	}
	public void HideDice()
	{
		foreach(PharoahDie d6 in diceList) {
			d6.gameObject.SetActive(false);
		}
	}
	public void UnhideDice()
	{
		foreach(PharoahDie d6 in diceList) {
			d6.gameObject.SetActive(true);
		}
	}
	public void StartTurn()
	{
		this.gameObject.SetActive (true);
		GameState.Message (this.name + " Start turn");
		if (pgs.curState == PlayerGameState.PlayerGameStates.WaitingNextTurn)
			pgs.SetState (PlayerGameState.PlayerGameStates.InitTurn);
		HideDice ();
        isFirstRoll = true;
    }

    public void LockDieRotations()
    {
        foreach(PharoahDie die in diceList)
        {
            die.LockRotation();
        }
    }

    public void AllowDieSelect()
    {
        foreach (PharoahDie die in diceList)
        {
            die.CanSelect();
        }
    }

    public void WaitForLock()
	{
		GameState.Message (this.name + " waiting for a locked die");
		pgs.SetState (PlayerGameState.PlayerGameStates.WaitingForLock);
        LockDieRotations();
        AllowDieSelect();
	}

	public void WaitForPurchase()
	{
		GameState.Message (this.name + " waiting to choose a tile");
		pgs.SetState (PlayerGameState.PlayerGameStates.WaitingForPurchaseTile);
	}
    public void UndoState()
    {
        PlayerGameState.PlayerGameStates newState = pgs.UndoState();
        GameState.Message(this.name + " Undo previous state. New State is " + newState.ToString());
    }

    public void TilePurchaseChosen()
	{
		GameState.Message (this.name + " tile chosen. Click dice cup to end turn.");
		pgs.SetState (PlayerGameState.PlayerGameStates.TilePurchaseChosen);
	}
	public void EndTurn()
	{
		GameState.Message (this.name + " turn has ended");
		this.gameObject.SetActive (false);
		foreach(PharoahDie d6 in diceList) {
			d6.ReadyToRoll();
			d6.EndTurn();
			d6.transform.parent = this.transform;
		}
        foreach(Tile tile in tileList) {
            tile.canUndo = false;
        }
		pgs.SetState (PlayerGameState.PlayerGameStates.WaitingNextTurn);
	}

	public void LockWhiteDice()
	{
		foreach (PharoahDie die in diceList) {
			if (die.isAutoLocking && !die.isLocked) {
				die.MoveToLockedArea();
				die.isLocked = true;
			}
		}
	}

    public void OnDoneClick()
    {
        pgs.OnDoneClick();
    }
    public void OnCancelClick()
    {
        pgs.OnCancelClick();
    }
    //  ***************** Tile ability stuff
    public void FireTriggers(TileAbility.PlayerTurnStateTriggers trigState)
    {
        foreach(Tile tile in tileList)
        {
            tile.FireTrigger(trigState, this);
        }
    }
}
