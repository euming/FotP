﻿using UnityEngine;
using System.Collections;
using System.Collections.Generic;

//	This maintains the database of how card
//	functionality attaches to the card graphics.

[ExecuteInEditMode]

public class BarDatabase : TileMapDatabase {
	
	//	names are stored in Unity's database object rather than here.
	//public List<string> names;
	
	void Awake() {
		int idx = 0;
		for(int row=9; row>=0; --row) {
			for(int col=0; col<1; ++col) {
				pos[idx].x = row;
				pos[idx].y = col;
				idx++;
			}
		}
	}
	
	// Use this for initialization
	void Start () {
	}
	
	// Update is called once per frame
	void Update () {
	}
}
