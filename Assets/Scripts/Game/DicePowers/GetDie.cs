using UnityEngine;
using System.Collections;

public class GetDie : TileAbility {

	public int 					setDieValue;	//	-1 for none
	public DiceFactory.DieType 	type;

	PharoahDie					myDie;

	//	does something when we acquire this tile
	public override void OnAcquire(PlayerBoard plr)
	{
		PharoahDie die = plr.AddDie(type);
		if (setDieValue > 0) {
			die.MakeSetDie(setDieValue);
		}
        die.PurchasedDie();
        myDie = die;
	}

	public override void OnAcquireUndo(PlayerBoard plr)
	{
		plr.DestroyDie(myDie);
		myDie = null;
	}
}
