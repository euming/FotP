using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class TileFactory : MonoBehaviour {

	public List<Tile> YellowTiles3;
	public List<Tile> YellowTiles4;
	public List<Tile> YellowTiles5;
	public List<Tile> YellowTiles6;
	public List<Tile> YellowTiles7;

	public List<Tile> GetYellowTileList(int rank)
	{
		switch (rank)
		{
		default:
			break;
		case 3:
			return YellowTiles3;
		case 4:
			return YellowTiles4;
		case 5:
			return YellowTiles5;
		case 6:
			return YellowTiles6;
		case 7:
			return YellowTiles7;
		}
		return null;
	}

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}
