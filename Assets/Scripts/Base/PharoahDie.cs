using UnityEngine;
using System;
using System.Collections;
using System.Collections.Generic;

public class PharoahDie : Die_d6, IComparable<PharoahDie> {
    public bool isTempLocked = false;   //	when we want to lock this at the end of the turn, but have the option to undo it
    public bool isLocked = false;
    public bool isAutoLocking = false;	//	this autolocks (white dice are immediate dice)
    bool onMoveCompleteUnslot = false;   //  when we're done moving, unslot (this is for moving to dice cup)
    bool isUndoable = false;        //	can we undo?

    public DiceFactory.DieType type;
    public int setDieValue; //	if >0, then this die is set when we roll and not rolled from the cup.
    int tempPips = 0;
    int origSide;
    bool bIsTemporary = false;

    DieSlot mySlot;
    GameObject spawnPoint = null;

    void Awake() {
        spawnPoint = GameObject.Find("rollingDiceSpawnPoint");
        if (spawnPoint == null) {
            Debug.LogError("rollingDiceSpawnPoint not found! Cannot spawn dice.");
        }
    }

    // Use this for initialization
    void Start() {
        if (this.type == DiceFactory.DieType.White) {
            isAutoLocking = true;
        }
        //iTween.Init (this.gameObject);
    }

    // Update is called once per frame
    void Update() {

    }

    //	IComparable
    // Default comparer. Sorts from high to low.
    public int CompareTo(PharoahDie die)
    {
        if (die == null) {
            return -1;
        }
        else {
            if (this.GetValue() == die.GetValue())
                return 0;
            else if (this.GetValue() < die.GetValue())
                return 1;
            else
                return -1;
        }
    }

    static public void SortList(List<PharoahDie> diceList)
    {
        diceList.Sort();
    }

    //	make this die into a set die. done after instantiate
    public void MakeSetDie(int setDieValue)
    {
        this.setDieValue = setDieValue;
        this.SetDie(setDieValue);
        this.isTempLocked = true;   //	start temp locked so that we can select it and move it
        this.MoveToSetDieArea();
    }

    public void MakeTemporary()
    {
        bIsTemporary = true;
    }

    public bool isTemporary()
    {
        return bIsTemporary;
    }

    public bool isSetDie()
    {
        if (setDieValue > 0) return true;
        return false;
    }

    //	take me out of whatever slot I'm in right now
    void Unslot()
    {
        if (mySlot) {
            mySlot.removeChild(this.gameObject);
            mySlot = null;
        }
    }

    //	itween callback stuff
    void OnMoveStart()
    {
        Debug.Log("iTween started " + this.name);
        Rigidbody rb = GetComponent<Rigidbody>();
        rb.detectCollisions = false;
        rb.useGravity = false;
    }
    void OnMoveComplete()
    {
        if (onMoveCompleteUnslot) {
            Unslot();
            this.gameObject.SetActive(false);
            onMoveCompleteUnslot = false;
        }

        Debug.Log("iTween completed " + this.name);
        Rigidbody rb = GetComponent<Rigidbody>();
        rb.detectCollisions = true;
        rb.useGravity = true;
    }

    void MoveToSlot(DieSlot ds)
    {
        Unslot();
        this.onMoveCompleteUnslot = ds.onMoveCompleteUnslot;    //  this die does whatever the dieSlot wants to do

        ds.addChild(this.gameObject);
        mySlot = ds;

        //Rigidbody rb = this.GetComponent<Rigidbody>();
        //rb.detectCollisions = false;
        //rb.constraints = RigidbodyConstraints.FreezeAll;
    }

    //	put this die into the locked area
    public void MoveToLockedArea()
    {
        GameState gs = GameState.GetCurrentGameState();

        DieSlot ds = gs.GetNextLockedDieSlot();
        MoveToSlot(ds);
        GameState.LockedDieThisTurn();
    }

    public void MoveToUnlockedArea()
    {
        if (this.isInLockedArea()) {
            GameState.UnlockedDieThisTurn();
        }
        GameState gs = GameState.GetCurrentGameState();

        DieSlot ds = gs.GetNextActiveDieSlot();
        MoveToSlot(ds);
    }

    public void MoveToSetDieArea()
    {
        GameState gs = GameState.GetCurrentGameState();

        DieSlot ds = gs.GetNextSetDieSlot();
        MoveToSlot(ds);
    }

    //  scale the die back to normal size. Hide the die.
    public void MoveToDiceCupArea()
    {

        DieSlot ds = GameState.GetCurrentGameState().diceCupSlot;
        MoveToSlot(ds);
        //Unslot();
        //Rigidbody rb = this.GetComponent<Rigidbody>();
        //rb.detectCollisions = false;

    }

    //  this is a die we just purchased. We can't do anything with it.
    public void PurchasedDie()
    {
        isLocked = true;
        isTempLocked = true;
        this.MoveToDiceCupArea();
        CannotSelect();

        //this.gameObject.SetActive(false);
    }

    //  after we roll this die, we should lock the rotation.
    public void LockRotation()
    {
        Rigidbody rb = GetComponent<Rigidbody>();
        rb.constraints |= RigidbodyConstraints.FreezeRotation;
    }

    //  allow this die to be reset for rolling
    public void ReadyToRoll()
    {
        onMoveCompleteUnslot = false;
        isLocked = false;
        isTempLocked = false;
        Unslot();
        //  reset scale
        this.transform.localScale = Vector3.one;
        CannotSelect();
    }

    //  move this die to the cup area
    public void PutDieInCup()
	{
        ReadyToRoll();
        MoveToDiceCupArea ();
		//MoveToUnlockedArea();
	}

	private void Unfreeze()
	{
		Rigidbody rb = this.GetComponent<Rigidbody> ();
		rb.useGravity = true;
		rb.constraints = RigidbodyConstraints.None;
	}
	private Vector3 Force()
	{
		Vector3 rollTarget = Vector3.zero + new Vector3(2 + 7 * UnityEngine.Random.value, .5F + 4 * UnityEngine.Random.value, -2 - 3 * UnityEngine.Random.value);
		return Vector3.Lerp(spawnPoint.transform.position, rollTarget, 1).normalized * (-35 - UnityEngine.Random.value * 20);
	}

    public void CannotSelect()
    {
        this.gameObject.layer = 2;  //  ignore raycast
    }

    public void CanSelect()
    {
        this.gameObject.layer = 8;  //  back to dice layer
    }

	public void RollDiePhysics()
	{
		Unslot ();
		Unfreeze ();
        CannotSelect();
		//Dice.Roll("1d6", "d6-red", spawnPoint.transform.position, Force());
		Dice.RollDie (this, spawnPoint.transform.position, Force ());
	}

	public void RollDie() {
		if (isLocked) return;	//	don't roll locked dice
		//	take me out of any slots I happen to be in.
		Unslot ();
		if (GameState.GetCurrentGameState().bUseDicePhysics) {
			RollDiePhysics();
		}
		else {
			SetDie (UnityEngine.Random.Range(1, 7));
			MoveToUnlockedArea();
			//if (isAutoLocking) {
			//	LockDie();
			//}
		}
	}

	public bool isInDiceArea(GameState.DiceAreaTags enm)
	{
		bool isInArea = false;
		if (this.mySlot != null) {
			if (this.mySlot.transform.parent != null) {
				string cmpString = GameState.DiceAreaTagStrings[(int)enm];
				Debug.Log("Compare="+cmpString);
				Debug.Log ("Tag="+this.mySlot.transform.parent.tag);
				if (this.mySlot.transform.parent.CompareTag(cmpString)) {
					isInArea = true;
				}
			}
		}
		return isInArea;
	}
	public bool isInLockedArea()
	{
		bool bIsInLockedArea = isInDiceArea(GameState.DiceAreaTags.LockedDiceArea);
		return bIsInLockedArea;
	}
	public bool isInActiveArea()
	{
		bool bIsInArea = isInDiceArea(GameState.DiceAreaTags.ActiveDiceArea);
		return bIsInArea;
	}
	public bool isInNoArea()
	{
		//bool bIsInNoArea = false;
		if (this.mySlot == null)
			return true;
		return false;
	}
	public bool isInSetDiceArea()
	{
		bool bIsInArea = isInDiceArea(GameState.DiceAreaTags.SetDiceArea);
		return bIsInArea;
	}

    //  Dice properties
    public bool isImmediateDie()
    {
        return this.isAutoLocking;
    }
    public bool isStandardDie()
    {
        bool isStandard = (this.type == DiceFactory.DieType.Red);
        return isStandard;
    }

    public bool isCustomDie()
    {
        bool isCustom = true;
        if (isImmediateDie())
            isCustom = false;
        if (isStandardDie())
        {
            isCustom = false;
        }
        return isCustom;
    }

    public bool isLockedDie()
    {
        bool bisLocked = isLocked || isTempLocked;
        return bisLocked;
    }

    public bool isActiveDie()
    {
        bool isActive = false;
        if (!isLockedDie() && !isInSetDiceArea())
        {
            isActive = true;
        }
        return isActive;
    }
    public bool isDieType(TileAbility.DieType onlyDieType)
    {
        bool isOfType = false;
        switch (onlyDieType)
        {
            default:
                break;
            case TileAbility.DieType.Any:
                isOfType = true;
                break;
            case TileAbility.DieType.Active:
                isOfType = this.isActiveDie();
                break;
            case TileAbility.DieType.Immediate:
                isOfType = isImmediateDie();
                break;
            case TileAbility.DieType.Custom:
                isOfType = isCustomDie();
                break;
            case TileAbility.DieType.Standard:
                isOfType = isStandardDie();
                break;
            case TileAbility.DieType.Locked:
                isOfType = isLockedDie();
                break;
            case TileAbility.DieType.ActiveCustomOrImmediate:
                isOfType = (isCustomDie() || isImmediateDie()) && isActiveDie();
                break;

        }
        return isOfType;
    }
    public void LockDie() {
		if (isInLockedArea()) return;

		//if (isAutoLocking)
		//	isLocked = true;
		//else
			isTempLocked = true;

		MoveToLockedArea();
	}

	//	unlock a die that is allowed to be unlocked.
	public void UnlockDie() {
		if (isTempLocked && !isLocked) {
			if (!this.isSetDie()) {
				isTempLocked = false;
			}
			MoveToUnlockedArea();
		}
	}

    //	tap to hide/unhide
    void OnMouseDown() {
        //  Game has determined that player needs to select a die now.
        if (GameState.GetCurrentGameState().currentPlayer.isSelectingDie())
        {
            GameState.GetCurrentGameState().currentPlayer.ChooseDie(this);
        }
        else  //  normal game state stuff
        {
            //  this is in the game play state
            if (isInLockedArea())
            {
                UnlockDie();
            }
            else if (isInSetDiceArea())
            {   //	move to active area
                MoveToUnlockedArea();
                isUndoable = true;  //	if we started here, we can undo and come back here.
            }
            else
            {   //	active area
                LockDie();
            }
        }
	}

	virtual public void OnRightClick(PlayerBoard currentPlayer) {
		if (isSetDie()) {
			if (this.isUndoable) {
				MoveToSetDieArea();
			}
		}
	}

	void OnMouseRightDown() {
		Debug.Log("OnMouseRightDown() - " + this.name);
		GameState gs = GameState.GetCurrentGameState();
		PlayerBoard currentPlayer = gs.currentPlayer;
		OnRightClick(currentPlayer);
	}
	
	//	detect right mouse click
	void OnMouseOver () {
		if(Input.GetMouseButtonDown(1)){
			OnMouseRightDown ();
		}
	}

    //  a single roll is finished after the player starts a new roll
    public void EndRoll()
    {
        if (isInLockedArea())
        {       //	set dice may be temp locked
            isLocked = true;        //	if we're in the locked area at the end of the turn, we become permanently locked
        }
        isTempLocked = false;
        isUndoable = false;

    }

    //  a turn is finished after the player has completed all rolls and chooses to pass play to the next player
    public void EndTurn() {
        EndRoll();
	}

    //  pip adjustments
    public void ClearTempPips()
    {
        tempPips = 0;
        origSide = GetSide();
    }

    //  this allows us to wrap around by having a negative tempPips value
    public void SetTempPipsValue(int val)
    {
        //  val = origSide + tempPips. tempPips = val - origSide.
        tempPips = val - origSide;
        this.SetDie(origSide + tempPips);
    }
    public void AddTempPips(int pips)
    {
        tempPips += pips;
        this.SetDie(origSide + tempPips);
    }

    public void UndoTempPips()
    {
        this.SetDie(origSide);
        tempPips = 0;
    }

    public void FinalizeTempPips()
    {
        this.SetDie(origSide + tempPips);
        tempPips = 0;
        origSide = GetSide();
        this.EndRoll();
    }

    public int getTempPips()
    {
        return tempPips;
    }
}
