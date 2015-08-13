using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[System.Serializable]

public class PlayerBoard : MonoBehaviour {

	public List<Tile>	tileList;	//	all my tiles

	public void NewGame()
	{
		tileList.Clear();
	}

	void Awake() {
	}
	// Use this for initialization
	void Start () {
	}
	
	// Update is called once per frame
	void Update () {
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
		return bSuccess;
	}

	public bool Drop(Tile tile)
	{
		bool bSuccess = false;
		if (Has (tile)) {
			bSuccess = true;
			Debug.Log(this.name + " drops " + tile.name);
			tileList.Remove (tile);
		}
		return bSuccess;
	}
}
