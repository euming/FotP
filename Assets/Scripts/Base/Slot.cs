using UnityEngine;
using System.Collections;

//	a slot is a Container that is more physical
[System.Serializable]
public class Slot : Container {
	public bool moveChild = false;	//	whether to move the child into the slot or not
	public bool scaleChild = false;	//	whether to scale the child according to the slot or not
	public bool orientChild = false;	//	whether to orient the child according to the slot or not
	public bool moveSlotToChild = false;	//	move the slot to the child.
	public bool animateMove = true;
	public float animTime = 0.8f;

	//	if I have a PositionToggler component, then I may have this slot move around depending upon the state.
	public int initialPositionToggleState = 0;	//	when the slot is empty, where do we put the object?
	public int curState;

	// Use this for initialization
	void Start () {
		curState = initialPositionToggleState;
		/*
		if (curState >= 0) {
			PositionToggler tglr = GetComponent<PositionToggler>();
			if (tglr != null) {
				tglr.SetState(curState);
			}
		}
		*/
	}
	
	// Update is called once per frame
	void Update () {
	}

	//	returns previous child that was in the slot, if any
	public override GameObject OnAddChild(GameObject child)
	{
		GameObject prevChild = null;
		if (child.transform.parent != null) {
			GameObject parentGO = child.transform.parent.gameObject;
			Container parentContainer = parentGO.GetComponent<Container>();
			if (parentContainer != this)
				prevChild = parentContainer.removeChild(child);
		}
		base.OnAddChild(child);
		if (moveSlotToChild) {
			this.transform.position = child.transform.position;
			this.transform.rotation = child.transform.rotation;
		}

		if (moveChild) {
			if (animateMove) {	//	we need to set the position to be where we currently are after we have this slot as the new parent. Then we will interpolate to zero directly on top of the slot.
				child.transform.localPosition = Vector3.zero;	//	put the child on top of the parent.
				//iTween.MoveToLocal(gameObject, Vector3.zero, animTime);
			}
			else {
				child.transform.localPosition = Vector3.zero;	//	put the child on top of the parent.
			}
		}
		if (scaleChild) {
			child.transform.localScale = Vector3.one;		//	allows the slot scale to scale the child as well.
		}
		if (orientChild) {
			child.transform.localRotation = Quaternion.identity;		//	allows the slot scale to rotate the child as well.
		}

		/*
		if (curState >= 0) {
			PositionToggler tglr = GetComponent<PositionToggler>();
			if (tglr != null) {
				tglr.SetState(curState);
			}
		}
		*/
		return prevChild;
	}
	
	public override void OnRemoveChild(GameObject child)
	{
		base.OnRemoveChild(child);
		curState = initialPositionToggleState;
	}
}
