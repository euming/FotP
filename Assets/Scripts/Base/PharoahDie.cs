using UnityEngine;
using System;
using System.Collections;
using System.Collections.Generic;

public class PharoahDie : Die_d6, IComparable<PharoahDie> {
	public bool isTempLocked = false;	//	when we want to lock this at the end of the turn, but have the option to undo it
	public bool isLocked = false;
	public bool isAutoLocking = false;	//	this autolocks (white dice are immediate dice)
	bool		isUndoable = false;		//	can we undo?

	public DiceFactory.DieType type;
	public int	setDieValue;	//	if >0, then this die is set when we roll and not rolled from the cup.

	DieSlot	mySlot;

	// Use this for initialization
	void Start () {
		if (this.type == DiceFactory.DieType.White) {
			isAutoLocking = true;
		}
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	//	IComparable
	// Default comparer. Sorts from high to low.
	public int CompareTo(PharoahDie die)
	{
		if (die == null) {
			return -1;
		}
		else {
			if (this.value == die.value)
				return 0;
			else if (this.value < die.value)
				return 1;
			else
				return -1;
		}
	}

	//	make this die into a set die. done after instantiate
	public void MakeSetDie(int setDieValue)
	{
		this.setDieValue = setDieValue;
		this.SetDie(setDieValue);
		this.isTempLocked = true;	//	start temp locked so that we can select it and move it
		this.MoveToSetDieArea();
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

	void MoveToSlot(DieSlot ds)
	{
		Unslot();
		ds.addChild(this.gameObject);
		mySlot = ds;
		
		Rigidbody rb = this.GetComponent<Rigidbody>();
		rb.constraints = RigidbodyConstraints.FreezeAll;
	}

	//	put this die into the locked area
	public void MoveToLockedArea()
	{
		GameState gs = GameState.GetCurrentGameState();

		DieSlot ds = gs.GetNextLockedDieSlot();
		MoveToSlot (ds);
	}

	public void MoveToUnlockedArea()
	{
		GameState gs = GameState.GetCurrentGameState();

		DieSlot ds = gs.GetNextActiveDieSlot();
		MoveToSlot(ds);
	}

	public void MoveToSetDieArea()
	{
		GameState gs = GameState.GetCurrentGameState();
		
		DieSlot ds = gs.GetNextSetDieSlot();
		MoveToSlot (ds);
	}

	public void PutDieInCup()
	{
		isLocked = false;
		isTempLocked = false;
		Unslot();
		MoveToUnlockedArea();
	}

	public void RollDie() {
		if (isLocked) return;	//	don't roll locked dice
		//	take me out of any slots I happen to be in.
		Unslot ();
		SetDie (UnityEngine.Random.Range(1, 7));
		if (isAutoLocking) {
			LockDie();
		}
		else {
			MoveToUnlockedArea();
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
	public bool isInSetDiceArea()
	{
		bool bIsInArea = isInDiceArea(GameState.DiceAreaTags.SetDiceArea);
		return bIsInArea;
	}
	public void LockDie() {
		if (isInLockedArea()) return;

		if (isAutoLocking)
			isLocked = true;
		else
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
		if (isInLockedArea()) {
			UnlockDie();
		}
		else if (isInSetDiceArea()) {	//	move to active area
			MoveToUnlockedArea();
			isUndoable = true;	//	if we started here, we can undo and come back here.
		}
		else {	//	active area
			LockDie();
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

	public void EndTurn() {
		if (isInLockedArea()) {		//	set dice may be temp locked
			isLocked = true;		//	if we're in the locked area at the end of the turn, we become permanently locked
		}
		isTempLocked = false;
		isUndoable = false;
	}
}
