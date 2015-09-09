using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[ExecuteInEditMode]
public class Bar : Toggler, IToggleReceiver {

	//	the 4 things that have a cost and what we can buy
	public List<BarSlot>	barSlotList;
	public PositionToggler	childBar;	//	the bar underneath us

	public int shopRow;

	public int curState;	//	0=closed, 1=open. This affects the barSlot positions/orientations

	void Awake() {
		int idx = 0;
		foreach(BarSlot slot in barSlotList) {
			slot.tileShopPos = new Vector2(idx, shopRow);
			++idx;
		}
		SetState (curState);
	}

	// Use this for initialization
	void Start () {
	}

	void OnDestroy() {
	}
	// Update is called once per frame
	void Update () {
	}

	public void SetState(int stateIdx)
	{
		curState = stateIdx;
		if (childBar != null) {
			childBar.SetState(curState);	//	sets the location of the bar underneath us.
		}
		//	sets the positions of the slots beneath us to match our current state
		foreach(BarSlot bs in barSlotList) {
			PositionToggler tglr = bs.GetComponent<PositionToggler>();
			if (tglr!=null) {
				tglr.SetState(curState);
			}
		}

	}

	public int Toggle()
	{
		curState ^= 1;
		SetState (curState);
		return curState;
	}

	override public void OnMouseDown() {
		//base.OnMouseDown();	//	do the list
		//	custom stuff. Toggle my child, but not myself
		Toggle();
	}
}
