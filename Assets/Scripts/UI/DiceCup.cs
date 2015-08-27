using UnityEngine;
using System.Collections;

public class DiceCup : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	void OnMouseDown() {
		Debug.Log("DiceCup.OnMouseDown()");
		GameState gs = GameState.GetCurrentGameState();
		PlayerBoard currentPlayer = gs.currentPlayer;
		currentPlayer.RollDice();
	}

	void OnMouseRightDown() {
		Debug.Log("DiceCup.RightClick()");
		GameState gs = GameState.GetCurrentGameState();
		PlayerBoard currentPlayer = gs.currentPlayer;
		currentPlayer.EndTurn();
	}

	//	detect right mouse click
	void OnMouseOver () {
		if(Input.GetMouseButtonDown(1)){
			OnMouseRightDown ();
		}
	}
}
