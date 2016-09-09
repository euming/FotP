using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class GetDie : TileAbility {

	public int 					setDieValue;	//	-1 for none
	public DiceFactory.DieType 	type;
    public bool                 isTemporary;        //  temporary dice are given and then taken away when the player's turn ends

    public override void OnSelect(PlayerBoard plr)
    {
        if (!isArtifactUsed)
        {
            base.OnSelect(plr);
            OnAcquire(plr); //  do the same thing as acquire.
        }
        else
        {
            GameState.Message("Artifact " + this.name + " has already been used this game.");
        }
    }
    //	does something when we acquire this tile
    public override void OnAcquire(PlayerBoard plr)
	{
        base.OnAcquire(plr);
        myDie = GetNewDie(plr);
        myDie.PurchasedDie(this);
	}

	public override void OnAcquireUndo(PlayerBoard plr)
	{
        base.OnAcquireUndo(plr);
		plr.DestroyDie(myDie);
		myDie = null;
	}

    //  get a new die that is ready to roll immediately
    PharoahDie GetNewDie(PlayerBoard plr)
    {
        PharoahDie die = plr.AddDie(type);
        myDie = die;    //  remember the die that this tileAbility bought. We may need to do something with it later.
        if (setDieValue > 0)
        {
            die.MakeSetDie(setDieValue);
        }
        if (isTemporary)
            die.MakeTemporary();

        return die;
    }
    public override void OnLockedAny(PlayerBoard plr)
    {
        base.OnLockedAny(plr);
        //  check to see if the player already has a Herder Die
        if (plr.hasHerderDie()) return; //  early bail. Probably print some sort of message and give the scarab tokens.

        //  check to see if the player has a locked pair
        //  if so, do the same thing as acquire
        if (isQualified())
        {
            myDie = GetNewDie(plr);
            plr.GiveHerderDie(myDie);
        }
    }
    
    //  NB: This was copied from BarSlot.cs. This could be made generic if we need criteria for other things eventually.
    //	if the locked dice have the proper dice to purchase this, then return true. return false otherwise.
    public bool isQualified()
    {
        bool isQual = false;
        PurchaseCriteria criteria = this.GetComponent<PurchaseCriteria>();

        if (criteria == null)
            return true;        //  early bail if no criteria needs to be met
            isQual = true;

        GameState gs = GameState.GetCurrentGameState();

        //  this checks only the locked dice list.
        List<PharoahDie> diceList = gs.GetLockedDiceList();
        if (criteria)
        {
            isQual = criteria.MatchesCriteria(diceList);
        }

        //  optional, check all dice on current player. May make things easier, but may make things more confusing.
        /*
		if (criteria) {
			isQual = criteria.MatchesCriteria(gs.currentPlayer.diceList);
		}
        */
        return isQual;
    }
}
