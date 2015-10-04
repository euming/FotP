using UnityEngine;
using System.Collections;
using System.Collections.Generic;
public class AllOdd : PurchaseCriteria {
	//	does this sorted list (high to low) of dice match the criteria that would allow purchase under this BarSlot
	public override bool MatchesCriteria(List<PharoahDie> sortedList) {
		if (CheckCheatMode()) return true;

		if (base.MatchesCriteria(sortedList) == false) return false;	//	bail on basics. Not enough dice.
		
		foreach(PharoahDie die in sortedList) {
			if (die.GetValue() % 2 == 0) return false;
		}
		return true;
	}
	
}
