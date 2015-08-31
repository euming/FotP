using UnityEngine;
using System.Collections;

public class PurchaseBoard : ToggleReceiver {

	public GameObject	tileDisplayBar;

	public PurchaseBoardState	curState;

	public enum PurchaseBoardState
	{
		isTuckedAway = 0,	//	hidden at the top
		//isCollapsed,	//	only relevant things can be seen
		isExpanded,		//	everything can be seen
		numOfPurchaseBoardStates,
	};

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	//	when tapped, this does something
	public int ChangeState()
	{
		curState++;
		if (curState >= PurchaseBoardState.numOfPurchaseBoardStates) {
			curState = 0;
		}
		switch (curState)
		{
		case PurchaseBoardState.isTuckedAway:
				break;
		/*
		case PurchaseBoardState.isCollapsed:
			this.gameObject.SetActive(true);
			tileDisplayBar.SetActive(false);
			break;
		*/
		case PurchaseBoardState.isExpanded:
			break;
		}
		return (int)curState;
	}

	//	receive the toggle.
	public override int Toggle()
	{
		ChangeState();
		return (int)curState;
	}
}
