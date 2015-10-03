using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[System.Serializable]
public class GameState : MonoBehaviour, IToggleReceiver {

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
        if (msg[msg.Length-1] != '\n')
            msg = msg + "\n";
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
	static public void StartTurn()
	{
		instance.currentPlayer.StartTurn();
	}

	static public void WaitForLock()	//	wait for player to lock a die
	{
		instance.currentPlayer.WaitForLock ();
	}
	static public void WaitForPurchase()
	{
		instance.purchaseBoard.SetState (PurchaseBoard.PurchaseBoardState.isExpanded);
		instance.currentPlayer.WaitForPurchase();
	}
	static public void EndTurn()
	{
		instance.currentPlayer.EndTurn ();
        Dice.Clear(false);
        instance.currentPlayer = instance.NextPlayer ();
		StartTurn ();
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
		StartTurn ();
	}
	// Use this for initialization
	void Start () {
		NewGame (allPlayers.Count);
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	int GetPlayerIndex(PlayerBoard match_plr)
	{
		int idx = -1;
		foreach(PlayerBoard plr in allPlayers) {
			idx++;
			if (plr == match_plr) {
				return idx;
			}
		}
		return idx;
	}
	PlayerBoard NextPlayer()
	{
		int idx = GetPlayerIndex (currentPlayer);
		idx++;
		if (idx >= allPlayers.Count)
			idx = 0;
		PlayerBoard nextPlr = allPlayers [idx];
		return nextPlr;
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

	public int Toggle()
	{
		this.CheatModeEnabled = !this.CheatModeEnabled;
		if (this.CheatModeEnabled) {
			GameState.Message("Cheat Mode Enabled!\nBuy any tile without restriction!");
		} else {
			GameState.Message("Cheat Mode disabled.\nPlay the game as normal now.");
		}
		if (this.CheatModeEnabled)
			return 1;
		return 0;
	}

}
