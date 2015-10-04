using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class NofaKind : PurchaseCriteria {

	public int numOfDiceOfaKind;

	//	does this sorted list (high to low) of dice match the criteria that would allow purchase under this BarSlot
	public override bool MatchesCriteria(List<PharoahDie> sortedList) {
		if (CheckCheatMode()) return true;

		if (base.MatchesCriteria(sortedList) == false) return false;	//	bail on basics. Not enough dice.

		int numMatching = 0;
		int lastValue = 0;

		foreach(PharoahDie die in sortedList) {
			if (die.GetValue() != lastValue) {
				numMatching = 1;
				lastValue = die.GetValue();
			}
			else {
				numMatching++;
			}
			if (numMatching >= this.numOfDiceOfaKind) {
				return true;
			}
		}
		return false;
	}
}
