using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Scarab : SelectableObject {
	public enum ScarabType
	{
		Reroll = 0,
		AddPip,
	};
	private ScarabType 		_scarabType;
    public PlayerGameState.delOnDieSelect onDieSelect;
    public bool isConsumed = false; //  if we've used this scarab. This lets our owner know when this object has been used up.

    public ScarabType type
    {
        get
        {
            return _scarabType;
        }
        set
        {
            _scarabType = value;
            SetDelegates();
        }
    }
    public void Awake()
    {
        onDieSelect = null;

    }
    public void Start()
    {
        SetDelegates();
    }
    public bool AddPip(PharoahDie die)
    {
        if (!die.isActiveDie())
        {
            GameState.Message("Cannot use Scarab on non-active die.");
            return false;
        }
        int val = die.GetValue();
        if (val+1 > die.MaxValue()) {
            GameState.Message("Cannot AddPip beyond die's max value of " + die.MaxValue().ToString());
            return false;
        }

        GameState.Message("Adding Pip to " + die.name);
        val++;
        if (val >= die.MaxValue())
            val = die.MaxValue();
        die.SetDie(val);
        isConsumed = true;
        return true;
    }

    public bool Reroll(PharoahDie die)
    {
        if (!die.isActiveDie())
        {
            GameState.Message("Cannot use Scarab on non-active die.");
            return false;
        }
        GameState.Message("Rerolling " + die.name);
        DiceCup.StartRolling();
        die.ReadyToRoll();
        die.RollDie();
        isConsumed = true;
        return true;
    }

    public void SetDelegates()
    {
        if (_scarabType == ScarabType.Reroll)
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
