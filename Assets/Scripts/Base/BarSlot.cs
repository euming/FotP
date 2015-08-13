using UnityEngine;
using System.Collections;

public class BarSlot : Slot {

	public Vector2		tileShopPos;	//	where we are in the tile shop
	public TileMapDatabase	tileDB;	//	which tile database we're using
	public int		nTiles;					//	number of tiles still available for sale

	public void NewGame()
	{
		nTiles = 1;	//	tbd

		//	tbd: roll a number and add the correct tile
		Tile	childTile;
		if (childTile = this.GetComponentInChildren<Tile>()) {
			addChild(childTile.gameObject);
			Debug.Log("BarSlot " + this.name + " added Tile " + childTile.name);
		}
		else {
			Debug.LogWarning("BarSlot " + this.name + " could not find a Tile on Start().");
		}
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

	public bool TakeOne()
	{
		bool bSuccess = false;
		if (nTiles > 0) {
			bSuccess = true;
			nTiles--;
		}
		return bSuccess;
	}

	public void ReturnOne()
	{
		nTiles++;
	}
}
