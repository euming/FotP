using UnityEngine;
using System.Collections;
using System.Collections.Generic;
	
	//	all dice must be greater or equal to the given value
public class AllDiceLessEqualTo : PurchaseCriteria {
		
	public int mustLessThanEqualTo;
	//	does this sorted list (high to low) of dice match the criteria that would allow purchase under this BarSlot
	public override bool MatchesCriteria(List<PharoahDie> sortedList) {
		if (CheckCheatMode()) return true;

		if (base.MatchesCriteria(sortedList) == false) return false;	//	bail on basics. Not enough dice.
		
		foreach(PharoahDie die in sortedList) {
			if (die.GetValue() > mustLessThanEqualTo) {
				return false;
			}
		}
		return true;
	}
}
