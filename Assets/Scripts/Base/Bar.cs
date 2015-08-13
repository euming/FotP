using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[ExecuteInEditMode]
public class Bar : Toggler {

	//	the 4 things that have a cost and what we can buy
	public List<BarSlot>	barSlotList;

	public int shopRow;

	void Awake() {
		int idx = 0;
		foreach(BarSlot slot in barSlotList) {
			slot.tileShopPos = new Vector2(idx, shopRow);
			++idx;
		}
	}

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}
