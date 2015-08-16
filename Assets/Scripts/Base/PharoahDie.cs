using UnityEngine;
using System.Collections;

public class PharoahDie : Die_d6 {
	public bool isTempLocked = false;	//	when we want to lock this at the end of the turn, but have the option to undo it
	public bool isLocked = false;
	public bool isWhiteDie = false;	//	this autolocks
	DieSlot	mySlot;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	//	take me out of whatever slot I'm in right now
	void Unslot()
	{
		if (mySlot) {
			mySlot.removeChild(this.gameObject);
			mySlot = null;
		}
	}
	//	put this die into the locked area
	void MoveToLockedArea()
	{
		GameState gs = GameState.GetCurrentGameState();

		DieSlot ds = gs.GetNextLockedDieSlot();
		Unslot();
		ds.addChild(this.gameObject);
		mySlot = ds;

		Rigidbody rb = this.GetComponent<Rigidbody>();
		rb.constraints = RigidbodyConstraints.FreezeAll;
	}

	void MoveToUnlockedArea()
	{
		GameState gs = GameState.GetCurrentGameState();

		DieSlot ds = gs.GetNextActiveDieSlot();
		Unslot();
		ds.addChild(this.gameObject);
		mySlot = ds;

		Rigidbody rb = this.GetComponent<Rigidbody>();
		rb.constraints = RigidbodyConstraints.FreezeAll;
	}

	public void PutDieInCup()
	{
		isLocked = false;
		isTempLocked = false;
	}

	public void RollDie() {
		if (isLocked) return;	//	don't roll locked dice
		if (mySlot) {
			mySlot.removeChild(this.gameObject);	//	take me out of any slots I happen to be in.
			mySlot = null;
		}
		SetDie (Random.Range(1, 7));
		if (isWhiteDie) {
			LockDie();
		}
		else {
			MoveToUnlockedArea();
		}
	}

	public bool isInLockedArea()
	{
		bool bIsInLockedArea = isLocked || isTempLocked;
		return bIsInLockedArea;
	}

	public void LockDie() {
		if (isInLockedArea()) return;

		if (isWhiteDie)
			isLocked = true;
		else
			isTempLocked = true;

		MoveToLockedArea();
	}

	public void UnlockDie() {
		if (isTempLocked && !isLocked) {
			isTempLocked = false;
			MoveToUnlockedArea();
		}
	}

	//	tap to hide/unhide
	void OnMouseDown() {
		if (isInLockedArea()) {
			UnlockDie();
		}
		else {
			LockDie();
		}
	}

	public void EndTurn() {
		if (isTempLocked)
			isLocked = true;
	}
}
