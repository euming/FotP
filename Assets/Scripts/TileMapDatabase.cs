using UnityEngine;
using System.Collections;
using System.Collections.Generic;

//	This maintains the database of how card
//	functionality attaches to the card graphics.

[ExecuteInEditMode]

public class TileMapDatabase : MonoBehaviour {
	
	//	eventually, we want to tie to the object directly rather than by name
	public List<string> names;
	
	protected Vector2[] pos = new Vector2[24];
	
	void Awake() {
	}
	
	// Use this for initialization
	void Start () {
	}
	
	// Update is called once per frame
	void Update () {
	}
	
	public string GetName(int row, int col)
	{
		for (int ii=0; ii<this.pos.Length; ++ii) {
			if (this.pos[ii].x == row) {
				if (this.pos[ii].y == col) {
					return names[ii];
				}
			}
		}
		return null;	//"Name Not Found";
	}
}
