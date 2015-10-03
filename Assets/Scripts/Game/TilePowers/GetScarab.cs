using UnityEngine;
using System.Collections;

//	gives the player a scarab when he takes the tile
public class GetScarab : TileAbility {

	Scarab					myScarab;	//	the scarab that is associated with this tile.
	Scarab.ScarabType		rndType;	//	save this state in case we undo acquiring this tile

    public override void OnStartTurn(PlayerBoard plr)
    {
        base.OnStartTurn(plr);
        OnAcquire(plr); //  do the same thing as acquire.
    }

    //	does something when we acquire this tile
    public override void OnAcquire(PlayerBoard plr)
	{
        base.OnAcquire(plr);
        rndType = (Scarab.ScarabType)Random.Range(0, 2);
        Scarab bug = plr.AddScarab(rndType);
		myScarab = bug;
        bug.name = "Scarab " + rndType.ToString();
    }

    public override void OnAcquireUndo(PlayerBoard plr)
	{
        base.OnAcquireUndo(plr);
		plr.DestroyScarab(myScarab);
		myScarab = null;
	}
}
