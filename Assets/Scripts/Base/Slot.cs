using UnityEngine;
using System.Collections;

//	a slot is a Container that is more physical
[System.Serializable]
public class Slot : Container {
	public bool moveChild = false;	//	whether to move the child into the slot or not
	public bool scaleChild = false;	//	whether to scale the child according to the slot or not
	public bool orientChild = false;	//	whether to orient the child according to the slot or not
	public bool moveSlotToChild = false;	//	move the slot to the child.
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
		base.OnAddChild(child);
		if (moveSlotToChild) {
			this.transform.position = child.transform.position;
			this.transform.rotation = child.transform.rotation;
		}

		if (moveChild) {
			child.transform.localPosition = Vector3.zero;	//	put the child on top of the parent.
		}
		if (scaleChild) {
			child.transform.localScale = Vector3.one;		//	allows the slot scale to scale the child as well.
		}
		if (orientChild) {
			child.transform.localRotation = Quaternion.identity;		//	allows the slot scale to rotate the child as well.
		}
		return prevChild;
	}
	
	public override void OnRemoveChild(GameObject child)
	{
		base.OnRemoveChild(child);
	}
}
