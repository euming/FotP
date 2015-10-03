using UnityEngine;
using System.Collections;

//	gives the player a scarab when he takes the tile
public class GetScarab : TileAbility {

	Scarab					myScarab;	//	the scarab that is associated with this tile.
	Scarab.ScarabType		rndType;	//	predetermined randomly by the tile.

	void Start()
	{
		rndType = (Scarab.ScarabType)Random.Range (0, 2);
	}

    public override void OnStartTurn(PlayerBoard plr)
    {
        base.OnStartTurn(plr);
        //Scarab bug = 
            plr.AddScarab(rndType);
        //myScarab = bug;
        //  do not allow undo
    }

    //	does something when we acquire this tile
    public override void OnAcquire(PlayerBoard plr)
	{
        base.OnAcquire(plr);
		Scarab bug = plr.AddScarab(rndType);
		myScarab = bug;
	}
	
	public override void OnAcquireUndo(PlayerBoard plr)
	{
        base.OnAcquireUndo(plr);
		plr.DestroyScarab(myScarab);
		myScarab = null;
	}
}
