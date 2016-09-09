using UnityEngine;
using System.Collections;
using System.Collections.Generic;

//	this is what grants the tile some ability.
public class TileAbility : MonoBehaviour {
    public enum PlayerTurnStateTriggers
    {
        NoTrigger,
        StartOfTurn,
        EndOfTurn,
        AllLocked,
        OnAcquireDie,       //  when the player gains a die
        OnSpecificRoll,     //  on a specific roll, this trigger fires.
        Acquire,            //  player has claimed this tile from the shop
        AcquireUndo,        //  player has returned this tile to the shop before turn end
        Select,             //  player has chosen this tile to use
        ChooseDie,          //  player has chosen a die
        LockedAny,         //  player has locked at least a single die (for the Herder tile)
        AllTrigger,         //  all triggers fire
    };

    public enum DieType
    {
        Standard,
        Immediate,
        Custom,
        Active,
        Locked,
        ActiveCustomOrImmediate,   //  for Royal Astrologer
        Any
    };

    protected PharoahDie myDie;

    public int  specificRoll;   //  abilities that trigger on a specific roll use this.
    public bool isArtifact;
	public bool isArtifactUsed;		//	Artifacts may be used once per game. Once used, we can't use it again
	public bool isUsedThisTurn;     //	true if we already used this ability this turn
    public List<PlayerTurnStateTriggers> fireOnTriggerList;
    public PlayerTurnStateTriggers onStateTrigger = PlayerTurnStateTriggers.AllTrigger;  //  on this state, trigger this ability

    /*
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
	*/
    public void SetMyDie(PharoahDie die)
    {
        myDie = die;
    }
    public virtual void OnAcquireDie(PharoahDie die)
    {
        GameState.Message("Tile " + this.name + " triggered OnAcquireDie PharoahDie " + die.name + "\n");
        myDie = die;
    }

    //  does something on the start of each turn
    public virtual void OnStartTurn(PlayerBoard plr)
    {
        GameState.Message("Tile " + this.name + " triggered OnStartTurn TileAbility " + this.GetType().ToString() + "\n");
    }
    
    public virtual void OnEndOfTurn(PlayerBoard plr)
    {
        GameState.Message("Tile " + this.name + " triggered OnEndOfTurn TileAbility " + this.GetType().ToString() + "\n");
    }

    public virtual void OnAllLocked(PlayerBoard plr)
    {
        GameState.Message("Tile " + this.name + " triggered OnAllLocked TileAbility " + this.GetType().ToString() + "\n");
    }
    //	does something when we acquire this tile
    public virtual void OnAcquire(PlayerBoard plr)
	{
        GameState.Message("Tile " + this.name + " triggered OnAcquire TileAbility " + this.GetType().ToString() + "\n");
    }

    //	if we change our mind and undo the acquire
    public virtual void OnAcquireUndo(PlayerBoard plr)
	{
        GameState.Message("Tile " + this.name + " triggered OnAcquireUndo TileAbility " + this.GetType().ToString() + "\n");
    }
    public virtual void OnSpecificDie(int dieRoll)
    {
        GameState.Message("Tile " + this.name + " triggered OnSpecificDie roll= " + dieRoll.ToString() + "\n");
    }
    //	does something when we select this tile
    public virtual void OnSelect(PlayerBoard plr)
	{
        GameState.Message("Tile " + this.name + " triggered OnSelect TileAbility " + this.GetType().ToString() + "\n");
		if (isArtifact)
			isArtifactUsed = true;
	}

    //	if we change our mind and undo the acquire
    public virtual void OnChooseDie(PlayerBoard plr)
    {
        GameState.Message("Tile " + this.name + " triggered OnChooseDie TileAbility " + this.GetType().ToString() + "\n");
    }

    //  if our player has a locked pair (actually, locked any die) LockedAny
    public virtual void OnLockedAny(PlayerBoard plr)
    {
        GameState.Message("Tile " + this.name + " triggered OnLockedAny TileAbility " + this.GetType().ToString() + "\n");
    }

    //  this calls the actual delegates for the triggers
    public virtual void ActualFireTrigger(PlayerTurnStateTriggers trig, PlayerBoard plr)
    {
        switch (trig)
        {
            default:
            case PlayerTurnStateTriggers.OnSpecificRoll:    //  my die has rolled. Send the trigger of what that roll was so that the TileAbility may react to it.
                int dieRoll = this.myDie.GetValue();
                OnSpecificDie(dieRoll);
                break;
            case PlayerTurnStateTriggers.NoTrigger:
                break;
            case PlayerTurnStateTriggers.StartOfTurn:
                OnStartTurn(plr);
                break;
            case PlayerTurnStateTriggers.EndOfTurn:
                OnEndOfTurn(plr);
                break;
            case PlayerTurnStateTriggers.AllLocked:
                OnAllLocked(plr);
                break;
            case PlayerTurnStateTriggers.Acquire:
                OnAcquire(plr);
                break;
            case PlayerTurnStateTriggers.AcquireUndo:
                OnAcquireUndo(plr);
                break;
            case PlayerTurnStateTriggers.Select:
                OnSelect(plr);
                break;
            case PlayerTurnStateTriggers.ChooseDie:
                OnChooseDie(plr);
                break;
            case PlayerTurnStateTriggers.LockedAny:
                OnLockedAny(plr);
                break;
        }
    }

    //  returns true - if trigger is allowed to fire its delegate
    //  returns false - if trigger is not selected as one of the allowed triggers
    public virtual bool FilterTriggers(PlayerTurnStateTriggers trig, PlayerBoard plr)
    {
        bool isAllowed = false;
        if (onStateTrigger==PlayerTurnStateTriggers.AllTrigger) isAllowed = true;
        if (trig == PlayerTurnStateTriggers.AcquireUndo) isAllowed = true;
        if (trig == onStateTrigger) isAllowed = true;
        foreach(PlayerTurnStateTriggers testTrig in fireOnTriggerList)
        {
            if (testTrig == trig) isAllowed = true;
        }
        return isAllowed;
    }
    
    //  received a type of trigger from somewhere. If this TileAbility is listening for it, then fire it off using the appropriate delegate
    //  AllTrigger - listens to all triggers and fires off delegates (if any) on all of them.
    //  AcquireUndo - is a system level trigger and must always fire.
    public virtual void FireTrigger(PlayerTurnStateTriggers trig, PlayerBoard plr)
    {
        /*  //  old logic
        if (onStateTrigger != PlayerTurnStateTriggers.AllTrigger)   //  if we trigger on all triggers, ignore this bail.
        {
            if (trig != PlayerTurnStateTriggers.AcquireUndo)  //  undo does not bail but always goes through
            {
                if (onStateTrigger != trig) return; //  bail if it's not the right trigger.
            }
        }
        */
        bool allowTriggerToFire = FilterTriggers(trig, plr);

        if (allowTriggerToFire)
            ActualFireTrigger(trig, plr);
    }
}
