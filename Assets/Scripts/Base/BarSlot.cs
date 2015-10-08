using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class BarSlot : Slot {

	public Vector2		tileShopPos;	//	where we are in the tile shop
	public TileMapDatabase	tileDB;	//	which tile database we're using
	public int		nTiles;					//	number of tiles still available for sale
	PurchaseCriteria		criteria;

	public void NewGame()
	{
		if (nTiles <= 0)
        {
            nTiles += GameState.GetCurrentGameState().allPlayers.Count;
        }

		//	tbd: roll a number and add the correct tile
		Tile	childTile;
		if (childTile = this.GetComponentInChildren<Tile>()) {
			addChild(childTile.gameObject);
			Debug.Log("BarSlot " + this.name + " added Tile " + childTile.name);
		}
		else {
			Debug.LogWarning("BarSlot " + this.name + " could not find a Tile on Start().");
		}
		criteria = GetComponent<PurchaseCriteria>();
	}

	// Use this for initialization
	void Start () {
		NewGame ();
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	public override GameObject OnAddChild(GameObject child)
	{
		GameObject prevChild = base.OnAddChild(child);
		Tile childTile = child.GetComponent<Tile>();
		childTile.mySlot = this;
		return prevChild;
	}

	public override void OnRemoveChild(GameObject child)
	{
		base.OnRemoveChild(child);
		Tile childTile = child.GetComponent<Tile>();
		childTile.mySlot = null;
	}

    public bool HasOne()
    {
        if (nTiles > 0) return true;
        return false;
    }
	public bool TakeOne()
	{
		bool bSuccess = false;
		if (nTiles > 0) {
			bSuccess = true;
			nTiles--;
		}
        if (nTiles==0)
        {
            this.myChild.GetComponent<Renderer>().enabled = false;
        }
		return bSuccess;
	}

	public void ReturnOne()
	{
		nTiles++;
        this.myChild.GetComponent<Renderer>().enabled = true;
    }

	//	if the locked dice have the proper dice to purchase this, then return true. return false otherwise.
	public bool isQualified()
	{
		bool	isQual = false;
		GameState gs = GameState.GetCurrentGameState();

        //  this checks only the locked dice list.
        List<PharoahDie> diceList = gs.GetLockedDiceList();
        if (criteria)
        {
            isQual = criteria.MatchesCriteria(diceList);
        }

        //  optional, check all dice on current player. May make things easier, but may make things more confusing.
        /*
		if (criteria) {
			isQual = criteria.MatchesCriteria(gs.currentPlayer.diceList);
		}
        */
		return isQual;
	}
}
