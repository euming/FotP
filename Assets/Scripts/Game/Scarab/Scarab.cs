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
    public PlayerGameState.delOnDieSelect onDieSelect;

    static public Scarab NewScarab(ScarabType type)
	{
		Scarab bug = GameObject.Instantiate(prefabScarab);

		return bug;
	}

    public void AddPip(PharoahDie die)
    {
        GameState.Message("Adding Pip to " + die.name);
    }

    public void Reroll(PharoahDie die)
    {
        GameState.Message("Rerolling " + die.name);
        DiceCup.StartRolling();
        die.ReadyToRoll();
        die.RollDie();
    }

    public void SetType(ScarabType newType)
    {
        type = newType;
        if (type== ScarabType.Reroll)
        {
            onDieSelect = Reroll;
        }
        else
        {
            onDieSelect = AddPip;
        }
    }
}
