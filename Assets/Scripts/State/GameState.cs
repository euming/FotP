using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[System.Serializable]
public class GameState : MonoBehaviour {

	static private GameState instance;
	static public GameState GetCurrentGameState() {return instance;}

	int							nPlayers;		//	number of players this game session
	public PlayerBoard			currentPlayer;
	public List<PlayerBoard> 	allPlayers;		
	public TileShop				tileShop;
	public List<DieSlot>		lockedDiceSlots;	//	my dice slots. Dice that are in the locked zone may be temporarily locked dice as well.
	public List<DieSlot>		activeDiceSlots;

	GameState()
	{
		instance = this;
	}

	// Use this for initialization
	void Start () {
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	public DieSlot GetNextLockedDieSlot()
	{
		foreach(DieSlot ds in lockedDiceSlots) {
			if (ds.isEmpty()) {
				return ds;
			}
		}
		return null;
	}

	public DieSlot GetNextActiveDieSlot()
	{
		foreach(DieSlot ds in activeDiceSlots) {
			if (ds.isEmpty()) {
				return ds;
			}
		}
		return null;
	}
}
