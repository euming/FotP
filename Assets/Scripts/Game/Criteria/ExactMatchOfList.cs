using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class ExactMatchOfList : PurchaseCriteria {
	public List<int>	exactMatchList;
	public List<bool>	matched;

	//	does this sorted list (high to low) of dice match the criteria that would allow purchase under this BarSlot
	public override bool MatchesCriteria(List<PharoahDie> sortedList) {
		if (CheckCheatMode()) return true;

		if (base.MatchesCriteria(sortedList) == false) return false;	//	bail on basics. Not enough dice.

		matched.Clear();
		matched.TrimExcess();

		foreach(PharoahDie die in sortedList) {
			int idx = matched.Count;
			if (die.value == exactMatchList[idx]) {	//	found one!
				matched.Add(true);
			}
		}

		//	we found them all!
		if (matched.Count == exactMatchList.Count) return true;

		return false;
	}
	
}
