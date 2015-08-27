using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Straight : PurchaseCriteria {
	public int maxStraightValue = 6;
	public int straightLength = 6;

	//	does this sorted list (high to low) of dice match the criteria that would allow purchase under this BarSlot
	public override bool MatchesCriteria(List<PharoahDie> sortedList) {
		if (CheckCheatMode()) return true;

		if (base.MatchesCriteria(sortedList) == false) return false;	//	bail on basics. Not enough dice.
		bool isValidStraight = false;
		int curLen = 0;
		int lastDieValue = -1;
		foreach(PharoahDie die in sortedList) {
			if (maxStraightValue == die.value) {
				curLen = 1;
				isValidStraight = true;
				lastDieValue = die.value;
			}
			if (isValidStraight) {
				if (die.value == lastDieValue - 1) {
					curLen++;
					lastDieValue = die.value;
				}
			}
		}
		if (isValidStraight) {
			if (curLen >= maxStraightValue) return true;
		}
		return false;
	}

}
