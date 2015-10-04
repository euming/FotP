using UnityEngine;
using System.Collections;
using System.Collections.Generic;

//	gives the player a scarab when he takes the tile
public class GetScarab : TileAbility {

	List<Scarab>			scarabList;	//	the scarab that is associated with this tile.
	Scarab.ScarabType		rndType;	//	save this state in case we undo acquiring this tile
    public bool doubleCurrentTokens_Ankh=false;   //  ankh ability
    public int nScarabs = 1;            //  gain this many scarabs

    void Awake()
    {
        scarabList = new List<Scarab>();
    }

    public override void OnStartTurn(PlayerBoard plr)
    {
        base.OnStartTurn(plr);
        OnAcquire(plr); //  do the same thing as acquire.
    }

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
        if (doubleCurrentTokens_Ankh)
        {
            nScarabs = plr.CountScarabs();
        }

        for (int ii = 0; ii < nScarabs; ii++)
        {
            rndType = (Scarab.ScarabType)Random.Range(0, 2);
            Scarab bug = plr.AddScarab(rndType);
            scarabList.Add(bug);
            bug.name = "Scarab " + rndType.ToString();
        }
    }

    public override void OnAcquireUndo(PlayerBoard plr)
	{
        base.OnAcquireUndo(plr);
        foreach(Scarab sc in scarabList)
        {
            plr.DestroyScarab(sc);
        }
	}
}
