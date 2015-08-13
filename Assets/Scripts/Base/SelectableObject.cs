using UnityEngine;
using System.Collections;

public class SelectableObject : DumbObject {

	//	Unity overloads
	// Use this for initialization
	void Start () {
		Debug.Log ("SelectableObject="+this.gameObject.name);
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	void OnMouseDown() {
		Debug.Log("SelectableObject.OnMouseDown()");
		GameState gs = GameState.GetCurrentGameState();
		PlayerBoard currentPlayer = gs.currentPlayer;
		OnSelect(currentPlayer);
	}

	public virtual void OnSelect(PlayerBoard currentPlayer) {
		Debug.Log("SelectableObject.OnSelect():" + this.gameObject.name + " by: " + currentPlayer.name);
	}
}
