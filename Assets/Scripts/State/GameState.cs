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
}
