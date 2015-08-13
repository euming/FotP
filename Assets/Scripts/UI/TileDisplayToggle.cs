using UnityEngine;
using System.Collections;

public class TileDisplayToggle : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	//	tap to hide/unhide
	void OnMouseDown() {
		if (this.transform.parent) {
			PurchaseBoard pb = this.transform.parent.GetComponent<PurchaseBoard>();
			if (pb) {
				pb.ChangeState();
			}
		}
	}
}
