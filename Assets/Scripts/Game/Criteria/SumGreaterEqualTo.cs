using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class SumGreaterEqualTo : PurchaseCriteria {

	public int minSum = 15;

	//	does this sorted list (high to low) of dice match the criteria that would allow purchase under this BarSlot
	public override bool MatchesCriteria(List<PharoahDie> sortedList) {
		if (CheckCheatMode()) return true;

		if (base.MatchesCriteria(sortedList) == false) return false;	//	bail on basics. Not enough dice.

		int sum = 0;
		foreach(PharoahDie die in sortedList) {
			sum += die.value;
			if (sum >= minSum) return true;
		}
		return false;
	}

}
