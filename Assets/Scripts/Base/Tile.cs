﻿using UnityEngine;
using System.Collections;
using System.Collections.Generic;

//	a tile is one of things you buy that gives you abilties to help you win the game
public class Tile : SelectableObject {

	public BarSlot		mySlot;		//	where I belong before I'm bought. I may have to go back here if the player changes his mind before hitting DONE.
	public int		shopRow;	//	index of shop row
	public List<int> shopCol;	//	index of valid shop columns

	//	figure out which slot I should be in, and put myself there.
	void AutoAssignToSlot()
	{
		GameState		gs = GameState.GetCurrentGameState();
		TileShop 		ts = gs.tileShop;

		//	am I already in a valid slot? if so, bail
		if (mySlot != null) {	//	we already are in a slot
			//	is this the slot that we are supposed to be in anyway?
			if (isValidSlot(mySlot)) {
				//	if so, then don't do anything else because we have already successfully "autoassigned" to this slot
				return;
			}
			mySlot.removeChild(this.gameObject);	//	remove this from the slot
		}

		//	first get the bar according to the shop Row index
		Bar bar = ts.barList[shopRow];
		foreach(int colIdx in shopCol) {
			BarSlot slot = bar.barSlotList[colIdx];
			if (slot.isEmpty()) {
				slot.addChild(this.gameObject);
				break;	//	we're done
			}
		}
	}


	// Use this for initialization
	void Start () {
		AutoAssignToSlot();
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	//	see if this slot is one of the valid slots we have designated in the Unity data properties.
	public bool isValidSlot(BarSlot testSlot)
	{
		GameState		gs = GameState.GetCurrentGameState();
		TileShop 		ts = gs.tileShop;

		Bar bar = ts.barList[shopRow];
		foreach(int colIdx in shopCol) {
			BarSlot slot = bar.barSlotList[colIdx];
			if (slot == testSlot) {
				return true;
			}
		}
		return false;
	}

	//	go back to where I came from
	void ReturnToSlot()
	{
		mySlot.ReturnOne();
	}

	public override void OnSelect(PlayerBoard currentPlayer) {
		base.OnSelect(currentPlayer);
		if (currentPlayer.Has(this)) {
			this.ReturnToSlot();
			currentPlayer.Drop(this);
		}
		else {
			bool bGotOne = mySlot.TakeOne();
			if (bGotOne) {
				currentPlayer.Take(this);
			}
			else {
				Debug.Log(mySlot.name + " is out of " + this.name + " so " + currentPlayer.name + " got none!");
			}
		}
	}
}
