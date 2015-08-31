using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[ExecuteInEditMode]
public class Bar : Toggler, IToggleReceiver {

	//	the 4 things that have a cost and what we can buy
	public List<BarSlot>	barSlotList;
	public PositionToggler	childBar;

	public int shopRow;

	public int curState;

	void Awake() {
		int idx = 0;
		foreach(BarSlot slot in barSlotList) {
			slot.tileShopPos = new Vector2(idx, shopRow);
			++idx;
		}
	}

	// Use this for initialization
	void Start () {
		childBar.SetState(curState);
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	public int Toggle()
	{
		if (childBar != null) {
			curState = childBar.Toggle();
		}
		foreach(BarSlot bs in barSlotList) {
			/*
			IToggleReceiver recv;
			recv = bs as IToggleReceiver;
			if (recv!=null) {
				recv.Toggle();
			}
			*/
			//	hackish: Just do the position toggler here. Could be more generic to run on all components, but I don't think we need that.
			PositionToggler tglr = bs.GetComponent<PositionToggler>();
			if (tglr!=null) {
				tglr.Toggle();
			}

		}
		return curState;
	}

	override public void OnMouseDown() {
		base.OnMouseDown();	//	do the list
		//	custom stuff. Toggle my child, but not myself
		Toggle();
	}
}
