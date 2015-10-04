using UnityEngine;
using System.Collections;

public class GetDie : TileAbility {

	public int 					setDieValue;	//	-1 for none
	public DiceFactory.DieType 	type;
    public bool                 isTemporary;        //  temporary dice are given and then taken away when the player's turn ends
	PharoahDie					myDie;

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
		PharoahDie die = plr.AddDie(type);
		if (setDieValue > 0) {
			die.MakeSetDie(setDieValue);
		}
        die.PurchasedDie();
        if (isTemporary)
            die.MakeTemporary();
        myDie = die;
	}

	public override void OnAcquireUndo(PlayerBoard plr)
	{
        base.OnAcquireUndo(plr);
		plr.DestroyDie(myDie);
		myDie = null;
	}
}
