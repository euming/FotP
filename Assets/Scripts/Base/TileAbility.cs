using UnityEngine;
using System.Collections;

//	this is what grants the tile some ability.
public class TileAbility : MonoBehaviour {
    public enum PlayerTurnStateTriggers
    {
        NoTrigger,
        StartOfTurn,
        EndOfTurn,
        AllLocked,
        Acquire,
        AcquireUndo,
        Select,             //  player has chosen this tile
        ChooseDie,          //  player has chosen a die
    };

    public bool isArtifact;
	public bool isArtifactUsed;		//	Artifacts may be used once per game. Once used, we can't use it again
	public bool isUsedThisTurn;     //	true if we already used this ability this turn
    public PlayerTurnStateTriggers onStateTrigger = PlayerTurnStateTriggers.Acquire;  //  on this state, trigger this ability

    /*
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
	*/

    //  does something on the start of each turn
    public virtual void OnStartTurn(PlayerBoard plr)
    {
        Debug.Log("Tile " + this.name + " triggered OnStartTurn TileAbility " + this.GetType().ToString() + "\n");
    }
    
    public virtual void OnEndOfTurn(PlayerBoard plr)
    {
        Debug.Log("Tile " + this.name + " triggered OnEndOfTurn TileAbility " + this.GetType().ToString() + "\n");
    }

    public virtual void OnAllLocked(PlayerBoard plr)
    {
        Debug.Log("Tile " + this.name + " triggered OnAllLocked TileAbility " + this.GetType().ToString() + "\n");
    }
    //	does something when we acquire this tile
    public virtual void OnAcquire(PlayerBoard plr)
	{
        Debug.Log("Tile " + this.name + " triggered OnAcquire TileAbility " + this.GetType().ToString() + "\n");
    }

    //	if we change our mind and undo the acquire
    public virtual void OnAcquireUndo(PlayerBoard plr)
	{
        Debug.Log("Tile " + this.name + " triggered OnAcquireUndo TileAbility " + this.GetType().ToString() + "\n");
    }

    //	does something when we select this tile
    public virtual void OnSelect(PlayerBoard plr)
	{
        Debug.Log("Tile " + this.name + " triggered OnSelect TileAbility " + this.GetType().ToString() + "\n");
        isUsedThisTurn = true;
		if (isArtifact)
			isArtifactUsed = true;
	}

    //	if we change our mind and undo the acquire
    public virtual void OnChooseDie(PlayerBoard plr)
    {
        Debug.Log("Tile " + this.name + " triggered OnChooseDie TileAbility " + this.GetType().ToString() + "\n");
    }

    public void FireTrigger(PlayerTurnStateTriggers trig, PlayerBoard plr)
    {
        if (onStateTrigger != trig) return; //  bail if it's not the right trigger.

        switch (trig)
        {
            default:
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
        }
    }
}
