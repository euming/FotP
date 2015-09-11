using UnityEngine;
using System.Collections;

public class DieSlot : Slot {

	// Use this for initialization
	void Start () {
	
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
		//base.OnAddChild(child);
		if (moveSlotToChild) {
			this.transform.position = child.transform.position;
			this.transform.rotation = child.transform.rotation;
		}
		
		if (moveChild) {
			if (animateMove) {	//	we need to set the position to be where we currently are after we have this slot as the new parent. Then we will interpolate to zero directly on top of the slot.
				Rigidbody rb = child.GetComponent<Rigidbody>();
				//rb.detectCollisions = false;
				//child.transform.localPosition = Vector3.zero;	//	put the child on top of the parent.
				iTween.MoveToLocal(child.gameObject, Vector3.zero, animTime);
				//	break child/parent relationship then reconnect it
				//GameObject parentGO = child.transform.parent.gameObject;
				//child.transform.parent = null;
				//iTween.MoveTo (child.gameObject, this.transform.position, 3.0f);
			}
			else {
				child.transform.localPosition = Vector3.zero;	//	put the child on top of the parent.
			}
		}
		if (scaleChild) {
			child.transform.localScale = Vector3.one;		//	allows the slot scale to scale the child as well.
		}
		if (orientChild) {
			Die_d6 die = child.GetComponent<Die_d6> ();
			if (die != null) {
				Rigidbody rb = die.GetComponent<Rigidbody>();
				//rb.detectCollisions = false;
				die.GetComponent<Rigidbody>().constraints = RigidbodyConstraints.FreezeRotation;
				die.OrientTowardLinedUp();
			} else {
				child.transform.localRotation = Quaternion.identity;		//	allows the slot scale to rotate the child as well.
			}
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
	

}
