using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class PurchaseCriteria : MonoBehaviour {

	public int minNumDice = 3;
	/*
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
	*/

	public virtual bool CheckCheatMode()
	{
//#if UNITY_EDITOR
		if (GameState.GetCurrentGameState().CheatModeEnabled) {
			GameState.Message ("Cheat mode allows purchase of any tile!");
			return true;
		}
//#endif
		return false;
	}

	//	does this sorted list (high to low) of dice match the criteria that would allow purchase under this BarSlot
	public virtual bool MatchesCriteria(List<PharoahDie> sortedList) {
		if (sortedList.Count < minNumDice) return false;
		return true;
	}
}
