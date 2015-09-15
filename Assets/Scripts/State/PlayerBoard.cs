using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[System.Serializable]

public class PlayerBoard : MonoBehaviour {

	public List<Tile>	tileList;	//	all my tiles
	public List<PharoahDie>	diceList;	//	all my dice
	public List<Scarab>		scarabList;		//	all my scarabs
	PlayerGameState pgs;

	public void NewGame()
	{
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
		Scarab bug = GameState.GetCurrentGameState().scarabPrefab;
		scarabList.Add(bug);
		return bug;
	}
	public void DestroyScarab(Scarab scarab)
	{
		scarabList.Remove(scarab);
		Destroy (scarab.gameObject);
	}

	//	add a new die to myself
	public PharoahDie AddDie(DiceFactory.DieType dieType)
	{
		PharoahDie die = GameState.GetCurrentGameState().diceFactory.NewDie(dieType);
		diceList.Add(die);
		die.PutDieInCup();
		return die;
	}

	//	========================================================================================
	//	dice stuff
	public void DestroyDie(PharoahDie die)
	{
		die.PutDieInCup();
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
		bool bSuccess = true;
		GameState.Message(this.name + " takes " + newTile.name);
		tileList.Add(newTile);
		newTile.OnAcquire(this);
		return bSuccess;
	}

	public bool Drop(Tile tile)
	{
		bool bSuccess = false;
		if (Has (tile)) {
			bSuccess = true;
			GameState.Message(this.name + " drops " + tile.name);
			tileList.Remove (tile);
			tile.OnAcquireUndo(this);
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
		PlayerGameState pgs = GetComponent<PlayerGameState> ();
		if (pgs != null) {
			bMayRoll = pgs.mayRollDice;
		}
		return bMayRoll;
	}

	public PlayerGameState.PlayerGameStates GetPlayerGameState()
	{
		PlayerGameState.PlayerGameStates pgse = PlayerGameState.PlayerGameStates.Uninitialized;
		PlayerGameState pgs = GetComponent<PlayerGameState> ();
		if (pgs != null) {
			pgse = pgs.curState;
		}
		return pgse;
	}
	//	return - were dice rolled or not?
	public bool RollDice()
	{
		if (!PlayerMayRollDice ()) {
			GameState.Message(this.name + " is in state " + GetPlayerGameState().ToString() + "\nand cannot roll dice yet.");
			return false;
		}
		int ndicerolled = 0;
		GameState.GetCurrentGameState ().purchaseBoard.SetState (PurchaseBoard.PurchaseBoardState.isTuckedAway);
		foreach(PharoahDie d6 in diceList) {
			d6.EndTurn();
			if (d6.isInActiveArea() || (d6.isInNoArea())) {
				d6.PutDieInCup();
				d6.RollDie();
				ndicerolled++;
			}
		}
		SortDiceList();
		GameState.Message (this.name + " rolling (" + ndicerolled.ToString() +"/"+diceList.Count.ToString() + ") dice");
		pgs.SetState (PlayerGameState.PlayerGameStates.DiceHaveBeenRolled);
		if (ndicerolled > 0)
			return true;
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
			d6.enabled = false;
		}
	}
	public void StartTurn()
	{
		this.gameObject.SetActive (true);
		GameState.Message (this.name + " Start turn");
		if (pgs.curState == PlayerGameState.PlayerGameStates.WaitingNextTurn)
			pgs.SetState (PlayerGameState.PlayerGameStates.InitTurn);
	}

	public void WaitForLock()
	{
		GameState.Message (this.name + " waiting for a locked die");
		pgs.SetState (PlayerGameState.PlayerGameStates.WaitingForLock);
	}
	public void EndTurn()
	{
		foreach(PharoahDie d6 in diceList) {
			d6.PutDieInCup();
		}
		this.gameObject.SetActive (false);
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
}
