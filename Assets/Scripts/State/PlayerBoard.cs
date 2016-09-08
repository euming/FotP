using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[System.Serializable]

public class PlayerBoard : MonoBehaviour {

	public List<Tile>	tileList;	//	all my tiles
	public List<PharoahDie>	diceList;	//	all my dice
	public List<Scarab>		scarabList;		//	all my scarabs
    public PlayerBoardUI    plrUI;             //  my UI for tiles

    Tile curTileInUse;              //  while we're using a tile, keep track of it.
    Scarab curScarabInUse;          //  while we're using the scarab, keep it separate from the others
    PlayerGameState pgs;
    bool isFirstRoll = false;
    bool bisStartPlayer;
    bool bHasExtraHerderDie = false;    //  true if the player has already gotten a herder die for this turn.

	public void NewGame()
	{
        curTileInUse = null;
        curScarabInUse = null;
        bisStartPlayer = false;
        bHasExtraHerderDie = false;
        tileList.Clear();
		diceList.Clear();
		for (int ii=0; ii<3; ++ii) {
			Die d6 = AddDie(DiceFactory.DieType.Red);
			d6.transform.parent = this.transform;
		}
		HideDice ();
		this.gameObject.SetActive (false);
	}

    public void SetStartPlayer()
    {
        bisStartPlayer = true;
    }

    public bool isStartPlayer()
    {
        return bisStartPlayer;
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
    public Scarab AddRandomScarab()
    {
        Scarab.ScarabType rndScarab = (Scarab.ScarabType)((int)(Random.value*2.0f));
        return AddScarab(rndScarab);
    }

	public Scarab AddScarab(Scarab.ScarabType scarabType)
	{
        GameState curGamestate = GameState.GetCurrentGameState();
        Scarab prefab = curGamestate.scarabPrefab;
        GameObject prefabGO = prefab.gameObject;
        GameObject bugGO = GameObject.Instantiate(prefabGO);
        GameState.Message("Instantiate bugGO");
        Scarab bug = bugGO.GetComponent<Scarab>();
        bug.type = scarabType;
		scarabList.Add(bug);
        bugGO.transform.parent = this.transform;    //  put this under the player board hierarchy.
        PlayerBoardAllUI.RefreshScarabUI();
        return bug;
	}
	public void DestroyScarab(Scarab scarab)
	{
		scarabList.Remove(scarab);
		Destroy (scarab.gameObject);
        PlayerBoardAllUI.RefreshScarabUI();
    }

    public Scarab hasScarabType(Scarab.ScarabType type)
    {

        foreach (Scarab sc in scarabList) {
            if (sc.type == type)
            {
                return sc;
            }
        }
        return null;
    }

    public int countScarabsOfType(Scarab.ScarabType type)
    {
        int nScarabs = 0;
        foreach (Scarab sc in scarabList)
        {
            if (sc.type == type)
            {
                nScarabs++;
            }
        }
        return nScarabs;
    }
    public bool UseScarab(Scarab.ScarabType type)
    {
        bool bSuccess = false;
        Scarab hasScarab = hasScarabType(type);
        if (hasScarab!=null) {
            curScarabInUse = hasScarab;
            scarabList.Remove(hasScarab);   //  remove my scarab from the list and hold it in curScarabInUse until we've decided what happens to it. Consumed or Undo.
            PlayerBoardAllUI.RefreshScarabUI();
            //  this will wait until the player has selected a die, and then perform the scarab's delegate function on that die.
            this.AskToChooseDie(curScarabInUse.onDieSelect, type.ToString());
        }

        return bSuccess;
    }
    public int CountScarabs()
    {
        int nScarabs = 0;
        foreach (Scarab sc in scarabList)
        {
            if (sc != null)
            {
                nScarabs++;
            }
        }
        return nScarabs;
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

    //	========================================================================================
    //	tile stuff

    //	do we own a tile?
    public Tile Has(Tile tile)
	{
        Tile gotIt = null;
        foreach (Tile t in tileList) {
			if (t==tile) return t;
            if (t.myOriginal == tile) return t;
		}
		return gotIt;
	}

    void AddTile(Tile tile)
    {
        tileList.Add(tile);
        if (this.plrUI != null)
        {
            plrUI.AddTile(tile);
        }
    }
    void RemoveTile(Tile tile)
    {
        tileList.Remove(tile);
        if (this.plrUI != null)
        {
            plrUI.RemoveTile(tile);
        }
    }
    public bool Take(Tile claimedTile)
    {
        bool bSuccess = false;
        if (this.pgs.mayPurchaseTile) {
            GameState.Message(this.name + " claims " + claimedTile.name);
            Tile newTile = Instantiate(claimedTile.gameObject).GetComponent<Tile>();
            newTile.myOriginal = claimedTile;
            newTile.transform.parent = this.transform;
            AddTile(newTile);
            newTile.FireTrigger(TileAbility.PlayerTurnStateTriggers.Acquire, this);
            TilePurchaseChosen();
            bSuccess = true;
        }
        else
        {
            GameState.Message(this.name + " may not claim " + claimedTile.name);
        }
		return bSuccess;
	}

	public bool Drop(Tile tile)
	{
		bool bSuccess = false;
        Tile foundTile = Has(tile);
        if (foundTile!=null) {
            if (tile.canUndo) { 
                bSuccess = true;
                GameState.Message(this.name + " returns " + tile.name);
                RemoveTile(foundTile);
                foundTile.FireTrigger(TileAbility.PlayerTurnStateTriggers.AcquireUndo, this);
                Destroy(foundTile.gameObject);
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
        PharoahDie.SortList(diceList);
	}

    //  query the player game state for whether this player may roll the dice or not.
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
        if (del==null)
        {
            Debug.LogError("No delegate was defined, so selecting a die will do nothing.");
        }
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
        bool bSuccessfulDieChosen = pgs.ChooseDie(die); //  calls OnDieSelect delegate. For scarabs, this will reroll or addpip. For TileAbility, it will call the ability's delegate, if any

        if (bSuccessfulDieChosen)
        {
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
        else//  we were not able to select a valid die
        {
            if (this.curScarabInUse)
            {
                //  return the scarab back to our list without consuming it
                scarabList.Add(this.curScarabInUse);
                PlayerBoardAllUI.RefreshScarabUI();
                this.curScarabInUse = null;        
            }
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

    //  roll a single die
    public bool RollDie(PharoahDie die)
    {
        if (die == null) return false;
        GameState.GetCurrentGameState().purchaseBoard.SetState(PurchaseBoard.PurchaseBoardState.isTuckedAway);
        UnhideDice();
        DiceCup.StartRolling();
        die.ReadyToRoll();
        die.RollDie();
        return true;
    }

    //  when we start a new roll, some state changes need to happen.
    void InitRoll()
    {
        if (pgs)
        {
            pgs.StartTurn();
            //pgs.diceLockedThisTurn = 0;
            //pgs.lastDiceLockedThisTurn = 0;
        }
    }

    // RollDice signals the end of the last roll and the beginning of the next one.
    //	return - were dice rolled or not?
    bool bForcePass = false;
	public bool RollDice()
	{
        if (!isFirstRoll)
        {
            CheckEndOfRollTriggers();   //  some things trigger at the end of a roll. Only do this after the first roll
        }

        if (!PlayerMayRollDice ()) {    //  this signifies end of turn if the player is not allowed to roll dice.
			//	we can't roll dice because we just purchased a new tile. We can only end the turn here.
			if (pgs.curState == PlayerGameState.PlayerGameStates.TilePurchaseChosen) {
				GameState.EndTurn();
				return false;
			}
			GameState.Message(this.name + " is in state " + GetPlayerGameState().ToString() + "\nand cannot roll. Click again to pass turn.");
			if (bForcePass) {
                //  add two scarabs when you don't purchase a tile
                AddRandomScarab();
                AddRandomScarab();
                GameState.EndTurn();
			}
			bForcePass = true;
			return false;
		}   //  end player may not roll dice

        //  able to roll dice, continue
		bForcePass = false;
		int ndicerolled = 0;
		GameState.GetCurrentGameState().purchaseBoard.SetState (PurchaseBoard.PurchaseBoardState.isTuckedAway);
        GameState.GetCurrentGameState().playerBoardAllUI.SetState(PlayerBoardAllUI.PlayerBoardAllUIState.isExpanded);
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
		GameState.Message (this.name + " rolling (" + ndicerolled.ToString() +"/"+diceList.Count.ToString() + ") dice");
		if (ndicerolled > 0) {
			pgs.SetState (PlayerGameState.PlayerGameStates.DiceHaveBeenRolled);
			return true;
		}
        SortDiceList();
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
        bHasExtraHerderDie = false;
    }

    public bool hasHerderDie()
    {
        return bHasExtraHerderDie;
    }

    public void GiveHerderDie(PharoahDie die)
    {
        bHasExtraHerderDie = true;
        pgs.mayRollDice = true; //  if we JUST got a herder die, we are always allowed a roll of the herder die, even if we just locked all of our other dice.
        GameState.Message(this.name + " received a Herder die!");
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

        //  End of Turn Triggers
        foreach (Tile tile in tileList)
        {
            TileAbility ability = tile.GetComponent<TileAbility>();
            if (ability)
            {
                //  Herder ability. If the player has locked a pair, then give the player a die for the remainder of the turn.
                if (ability.onStateTrigger == TileAbility.PlayerTurnStateTriggers.LockedAny)
                {

                }
            }
        }

        //  for all dice - remove all temporary dice
        for (int ii = diceList.Count-1; ii >= 0; ii--)    //  reverse remove so we don't get weird list problems
        {
            PharoahDie d6 = diceList[ii];
            if (d6.isTemporary())   //  remove temporary dice
            {
                diceList.RemoveAt(ii);    //  take this die out of this list
                Destroy(d6.gameObject);   //    destroy this whole die.
            }
        }
        /*
        foreach (PharoahDie d6 in diceList)
        {
            if (d6.isTemporary())   //  remove temporary dice
            {
                diceList.Remove(d6);
                Destroy(d6.gameObject);
            }
        }
        */
        //  for all dice - reset all permanent dice
        foreach (PharoahDie d6 in diceList) {
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

    void CheckEndOfRollTriggers()
    {
        if (pgs && (pgs.diceLockedThisTurn >= 1))
        {
            FireTriggers(TileAbility.PlayerTurnStateTriggers.LockedAny);
        }
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
