using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Scarab : SelectableObject {
	public enum ScarabType
	{
		Reroll,
		AddPip,
	};
	static public Scarab			prefabScarab = GameState.GetCurrentGameState().scarabPrefab;
	public ScarabType 		type;

	static public Scarab NewScarab(ScarabType type)
	{
		Scarab bug = GameObject.Instantiate(prefabScarab);

		return bug;
	}
}
