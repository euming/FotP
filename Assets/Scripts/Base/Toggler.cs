using UnityEngine;
using System.Collections;
using System.Collections.Generic;
public class Toggler : MonoBehaviour {

	public List<ToggleReceiver>		toggleReceivers;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	//	tap to hide/unhide
	void OnMouseDown() {
		foreach(ToggleReceiver recv in toggleReceivers) {
			recv.Toggle();
		}
	}

}
