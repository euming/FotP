using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Scarab : SelectableObject {
	public enum ScarabType
	{
		Reroll = 0,
		AddPip,
	};
	private ScarabType 		scarabType;
    public PlayerGameState.delOnDieSelect onDieSelect;
    public bool isConsumed = false; //  if we've used this scarab. This lets our owner know when this object has been used up.

    public ScarabType type
    {
        get
        {
            return scarabType;
        }
        set
        {
            scarabType = type;
            SetDelegates();
        }
    }
    public void Awake()
    {
        GameState.Message("Scarab.Awake()");
        onDieSelect = null;

    }
    public void Start()
    {
        GameState.Message("Scarab.Start()");
        SetDelegates();
    }
    public bool AddPip(PharoahDie die)
    {
        if (!die.isActiveDie()) {
            return false;
        }
        GameState.Message("Adding Pip to " + die.name);
        int val = die.GetValue();
        val++;
        if (val >= 6)
            val = 6;
        die.SetDie(val);
        isConsumed = true;
        return true;
    }

    public bool Reroll(PharoahDie die)
    {
        if (!die.isActiveDie())
        {
            return false;
        }
        GameState.Message("Rerolling " + die.name);
        DiceCup.StartRolling();
        die.ReadyToRoll();
        die.RollDie();
        isConsumed = true;
        return true;
    }

    //public void SetScarabType(ScarabType newType)
    //{
    //    scarabType = newType;
    //    //if (type == ScarabType.Reroll)
    //    //{
    //    //    this.name = "Scarab Reroll";
    //    //}
    //    //else
    //    //{
    //    //    this.name = "Scarab AddPip";
    //    //}
    //    //SetDelegates();
    //}

    public void SetDelegates()
    {
        if (scarabType == ScarabType.Reroll)
        {
            onDieSelect = Reroll;
        }
        else
        {
            onDieSelect = AddPip;
        }

    }
}
