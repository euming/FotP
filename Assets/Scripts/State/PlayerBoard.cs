using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[System.Serializable]

public class PlayerBoard : MonoBehaviour {

	public List<Tile>	tileList;	//	all my tiles
	public List<PharoahDie>	diceList;	//	all my dice
	public List<Scarab>		scarabList;		//	all my scarabs
	public int hasLockedThisTurn = 0;	//	number of dice locked this turn

	public void NewGame()
	{
		tileList.Clear();
		hasLockedThisTurn = 0;
	}

	void Awake() {
	}
	// Use this for initialization
	void Start () {
		foreach(PharoahDie d6 in diceList) {
			d6.PutDieInCup();
		}
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
		Debug.Log(this.name + " takes " + newTile.name);
		tileList.Add(newTile);
		newTile.OnAcquire(this);
		return bSuccess;
	}

	public bool Drop(Tile tile)
	{
		bool bSuccess = false;
		if (Has (tile)) {
			bSuccess = true;
			Debug.Log(this.name + " drops " + tile.name);
			tileList.Remove (tile);
			tile.OnAcquireUndo(this);
		}
		return bSuccess;
	}

	public void SortDiceList()
	{
		diceList.Sort();
	}

	public void RollDice()
	{
		GameState.GetCurrentGameState ().purchaseBoard.SetState (PurchaseBoard.PurchaseBoardState.isTuckedAway);
		foreach(PharoahDie d6 in diceList) {
			d6.EndTurn();
			if (d6.isInActiveArea() || (d6.isInNoArea())) {
				d6.PutDieInCup();
				d6.RollDie();
			}
		}
		SortDiceList();
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
	public void EndTurn()
	{
		foreach(PharoahDie d6 in diceList) {
			d6.PutDieInCup();
		}
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
