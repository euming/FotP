using UnityEngine;
using System.Collections;

public class PurchaseBoard : MonoBehaviour, IToggleCallback {

	public GameObject	tileDisplayBar;

	public PurchaseBoardState	curState;
	PositionToggler				posToggler;

	public enum PurchaseBoardState
	{
		isTuckedAway = 0,	//	hidden at the top
		//isCollapsed,	//	only relevant things can be seen
		isExpanded,		//	everything can be seen
		numOfPurchaseBoardStates,
	};

	void Awake() {
	}

	// Use this for initialization
	void Start () {
		posToggler = GetComponent<PositionToggler> ();
		posToggler.onToggleRecvrs.Add (this);
	}
	
	// Update is called once per frame
	void Update () {
	
	}
	public int SetState(PurchaseBoardState newState)
	{
		curState = newState;
		posToggler.SetState ((int)newState);
		switch (curState)
		{
		case PurchaseBoardState.isTuckedAway:
			break;
		case PurchaseBoardState.isExpanded:
			//	need to put any loose dice in the active dice area
			GameState.GetCurrentGameState().currentPlayer.CollectLooseDice();
			break;
		}
		return (int)newState;
	}
	//	when tapped, this does something
	public int ChangeState()
	{
		curState++;
		if (curState >= PurchaseBoardState.numOfPurchaseBoardStates) {
			curState = 0;
		}
		SetState (curState);
		return (int)curState;
	}

	/*
	//	receive the toggle.
	public override int Toggle()
	{
		//ChangeState();
		curState = (PurchaseBoardState)posToggler.curIndex;
		SetState (curState);
		return (int)curState;
	}
	*/
	public void OnToggle(int curIndex)
	{
		curState = (PurchaseBoardState)curIndex;
		SetState (curState);
	}
}
