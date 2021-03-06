﻿using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class DiceCup : MonoBehaviour {

	public List<Collider> walls;
	static DiceCup s_instance;

	// Use this for initialization
	void Start () {
		s_instance = this;
		DeactivateWalls ();
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	//	dice now collide with walls
	public void ActivateWalls()
	{
		foreach (Collider wall in walls) {
			wall.enabled = true;
		}
	}
    public static void StartRolling()
    {
        if (s_instance)
        {
            s_instance.ActivateWalls();
        }

    }
    //	when the dice have stopped rolling, this is called.
    public static void StopRolling()
	{
		if (s_instance) {
			s_instance.DeactivateWalls();
		}
        //GameState.LockWhiteDice();
        GameState.DiceStoppedMoving();
		GameState.WaitForLock ();
	}

	public void DeactivateWalls()
	{
		foreach (Collider wall in walls) {
			wall.enabled = false;
		}
	}

    //   a click on the Dice Cup means the player may want to roll dice OR end the turn
	void OnMouseDown() {
		Debug.Log("DiceCup.OnMouseDown()");
		GameState gs = GameState.GetCurrentGameState();
		PlayerBoard currentPlayer = gs.currentPlayer;
		if (currentPlayer.RollDice ()) {    //  attempt to roll dice
			StartRolling();
		}
        //  otherwise, if no dice were rolled, it implies end of turn
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
