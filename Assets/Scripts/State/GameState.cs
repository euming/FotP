using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[System.Serializable]
public class GameState : MonoBehaviour {

	static public string[] DiceAreaTagStrings =
	{
		"ActiveArea",
		"LockedArea",
		"SetDiceArea",
		"InCupArea",
	};

	public enum DiceAreaTags
	{
		ActiveDiceArea,
		LockedDiceArea,
		SetDiceArea,
		InCupArea,
	};

	static private GameState instance;
	static public GameState GetCurrentGameState() {return instance;}
	public bool					CheatModeEnabled = false;	//	helpful for debugging.
	public bool					bUseDicePhysics = true;	//	actually roll the dice

	int							nPlayers;		//	number of players this game session
	public PlayerBoard			currentPlayer;
	public PurchaseBoard		purchaseBoard;	//	what we can buy in this game
	public List<PlayerBoard> 	allPlayers;		
	public TileShop				tileShop;
	public List<DieSlot>		lockedDiceSlots;	//	my dice slots. Dice that are in the locked zone may be temporarily locked dice as well.
	public List<DieSlot>		activeDiceSlots;
	public List<DieSlot>		setDiceSlots;		//	where set dice go
	public DieSlot				diceCupSlot;		//	for rolling dice
	public DiceFactory			diceFactory;
	public Scarab				scarabPrefab;
	public CanvasRenderer		statusMsg;

	static public void LockWhiteDice()
	{
		GameState gs = instance;
		gs.currentPlayer.LockWhiteDice();
	}

	static public void Message(string msg)
	{
		UnityEngine.UI.Text txt = instance.statusMsg.GetComponent<UnityEngine.UI.Text> ();
		txt.text = msg;
		Debug.Log(msg);
	}

	static public void LockedDieThisTurn()
	{
		instance.currentPlayer.LockedDieThisTurn ();
	}

	static public void UnlockedDieThisTurn()
	{
		instance.currentPlayer.UnlockedDieThisTurn ();
	}
	static public void WaitForLock()	//	wait for player to lock a die
	{
		instance.currentPlayer.WaitForLock ();
	}

	GameState()
	{
		instance = this;
	}

	void NewGame(int numPlrs)
	{
		Message ("Starting new game with " + numPlrs.ToString () + " players.");
		foreach (PlayerBoard plr in allPlayers) {
			plr.NewGame ();
		}
		int rndIndex = (int)(Random.value * 4.0f);
		currentPlayer = allPlayers [rndIndex];
		currentPlayer.StartTurn ();
	}
	// Use this for initialization
	void Start () {
		NewGame (allPlayers.Count);
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

	public DieSlot GetNextSetDieSlot()
	{
		foreach(DieSlot ds in setDiceSlots) {
			if (ds.isEmpty()) {
				return ds;
			}
		}
		return null;
	}
}
