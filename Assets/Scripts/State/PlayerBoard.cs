using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[System.Serializable]

public class PlayerBoard : MonoBehaviour {

	public List<Tile>	tileList;	//	all my tiles
	public List<PharoahDie>	diceList;	//	all my dice
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

	//	add a new die to myself
	public PharoahDie AddDie(DiceFactory.DieType dieType)
	{
		PharoahDie die = GameState.GetCurrentGameState().diceFactory.NewDie(dieType);
		diceList.Add(die);
		die.PutDieInCup();
		return die;
	}

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
		foreach(PharoahDie d6 in diceList) {
			d6.EndTurn();
			if (d6.isInActiveArea()) {
				d6.PutDieInCup();
				d6.RollDie();
			}
		}
		SortDiceList();
	}
		
	public void EndTurn()
	{
		foreach(PharoahDie d6 in diceList) {
			d6.PutDieInCup();
		}
	}
}
