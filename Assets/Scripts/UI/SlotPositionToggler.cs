using UnityEngine;
using System.Collections;

//	this is basically a position toggler, but exclusively used with slots
public class SlotPositionToggler : PositionToggler {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	override public void SetState(int idx)
	{
		base.SetState(idx);
		if ((idx >= 0) && (idx < positions.Count)) {
			curIndex = idx;

			if (bUseTween) {
				iTween.MoveToLocal(gameObject, positions[idx], animTime);
				iTween.RotateTo(gameObject, rotations[idx].eulerAngles, animTime);
			}
			else {
				this.transform.localPosition = positions[idx];
				this.transform.localRotation = rotations[idx];
			}
		}
	}
}
