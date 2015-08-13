using UnityEngine;
using System.Collections;

//	a slot is a Container that is more physical
[System.Serializable]
public class Slot : Container {

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
		//child.transform.localPosition = Vector3.zero;	//	put the child on top of the parent.
		//child.transform.localScale = Vector3.one;		//	allows the slot scale to scale the child as well.
		return prevChild;
	}
	
	public override void OnRemoveChild(GameObject child)
	{
		base.OnRemoveChild(child);
	}

	
}
