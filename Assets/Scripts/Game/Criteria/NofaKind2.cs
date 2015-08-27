using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class NofaKind2 : PurchaseCriteria {
	
	public List<int>numOfDiceOfaKind;	//	smallest group size
	public List<int>numOfDiceOfaKind2;	//	other valid equivalent criteria
	public List<int>numOfDiceOfaKind3;	//	largest group size

	public int totalSum;	//	if all of the dice were the same value, this is the number of dice which would satisfy the criteria
	List<int>completedGroups;
	List<int>groupIndices;

	public void Start()
	{
		totalSum = 0;
		foreach(int groupSize in numOfDiceOfaKind) {
			totalSum += groupSize;
		}
		completedGroups = new List<int>();
		groupIndices = new List<int>();
	}

	bool FindOpenGroup(List<int>diceGroup, int hasGroupSize, List<bool>usedGroups)
	{
		int idx = 0;
		bool bFound = false;
		foreach(int groupSize in diceGroup) {
			if (usedGroups[idx] != true) {
				if (groupSize >= hasGroupSize) {
					return true;
				}
			}
			idx++;
		}
		return bFound;
	}
	//	must have the lowest groups first. (i.e. 2,2,3) or (3,4)
	//	does this sorted list (high to low) of dice match the criteria that would allow purchase under this BarSlot
	public override bool MatchesCriteria(List<PharoahDie> sortedList) {
		if (CheckCheatMode()) return true;

		if (base.MatchesCriteria(sortedList) == false) return false;	//	bail on basics. Not enough dice.

		int numMatching = 0;
		int lastValue = 0;
		int maxNumMatching = 0;
		int idx;

		completedGroups.Clear ();
		groupIndices.Clear();
		//	first, count the completed groups
		foreach(PharoahDie die in sortedList) {
			if (die.value != lastValue) {	//	different than previous die
				if (numMatching > 0) {	//	make a new group
					completedGroups.Add(numMatching);
					groupIndices.Add(lastValue);
					if (numMatching > maxNumMatching) {
						maxNumMatching = numMatching;
					}
				}
				numMatching = 1;
				lastValue = die.value;
			}
			else {	//	same as previous die
				numMatching++;
			}
		}

		//	make a new group for the last one
		completedGroups.Add(numMatching);
		groupIndices.Add(lastValue);
		if (numMatching > maxNumMatching) {
			maxNumMatching = numMatching;
		}

		//	now, we will have some number of groupings. Depending upon our list of numOfDiceOfaKind conditions,
		//	we may have several valid groupings due to groupings of the same number. i.e. 5555 satisfies two pairs
		//	as well as a single triple.

		List<bool>markUsed = new List<bool>();
		//	example: Grouping (3,2,2) may be satisfied as (5,2) or (4,3) or (7+)
		idx = 0;
		foreach(int groupSize in completedGroups) {	//	we have some group that satisfies all of the criteria with one massive group of the same value
			Debug.Log("Group Size: " + groupSize + " " + groupIndices[idx].ToString()+"'s");
			markUsed.Add(false);
			idx++;
			if (groupSize >= totalSum) {
				return true;
			}
		}

		//	see if we can find a matching group!
		idx = 0;
		bool bGroupMatchSuccess = false;
		foreach(int groupSize in numOfDiceOfaKind3) {
			if (FindOpenGroup(completedGroups, groupSize, markUsed)) {
				markUsed[idx] = true;
				Debug.Log ("Success! Found a group of " + groupSize);
				bGroupMatchSuccess = true;
			}
			else {
				Debug.Log ("Couldn't find a group of " + groupSize);
				bGroupMatchSuccess = false;
				break;	//	couldn't find a match for this group
			}
			idx++;
		}

		//	if we found a fit, then return true, otherwise, keep trying other fits
		if (bGroupMatchSuccess) {
			return true;
		}

		idx = 0;
		bGroupMatchSuccess = false;
		foreach(int groupSize in numOfDiceOfaKind2) {
			if (FindOpenGroup(completedGroups, groupSize, markUsed)) {
				markUsed[idx] = true;
				Debug.Log ("Success! Found a group of " + groupSize);
				bGroupMatchSuccess = true;
			}
			else {
				Debug.Log ("Couldn't find a group of " + groupSize);
				bGroupMatchSuccess = false;
				break;	//	couldn't find a match for this group
			}
			idx++;
		}
		
		//	if we found a fit, then return true, otherwise, keep trying other fits
		if (bGroupMatchSuccess) {
			return true;
		}
		
		return false;
	}
}
