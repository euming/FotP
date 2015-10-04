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
    public bool isConsumed = false; //  if we've used this scarab. This lets our owner know when this object has been used up.

    static public Scarab NewScarab(ScarabType type)
	{
		Scarab bug = GameObject.Instantiate(prefabScarab);
        bug.SetType(type);
		return bug;
	}

    public void AddPip(PharoahDie die)
    {
        GameState.Message("Adding Pip to " + die.name);
        int val = die.GetValue();
        val++;
        if (val >= 6)
            val = 6;
        die.SetDie(val);
        isConsumed = true;
    }

    public void Reroll(PharoahDie die)
    {
        GameState.Message("Rerolling " + die.name);
        DiceCup.StartRolling();
        die.ReadyToRoll();
        die.RollDie();
        isConsumed = true;
    }

    public void SetType(ScarabType newType)
    {
        type = newType;
        if (type== ScarabType.Reroll)
        {
            onDieSelect = Reroll;
            this.name = "Scarab Reroll";
        }
        else
        {
            onDieSelect = AddPip;
            this.name = "Scarab AddPip";
        }
    }
}
