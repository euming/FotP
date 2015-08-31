using UnityEngine;
using System.Collections;
using System.Collections.Generic;
public class Toggler : MonoBehaviour {

	public List<MonoBehaviour>		toggleReceivers;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	//	tap to hide/unhide
	virtual public void OnMouseDown() {
		IToggleReceiver recv;
		//	toggle the stuff in my list
		foreach(MonoBehaviour mb in toggleReceivers) {
			Component[] comps = mb.GetComponents<Component>();
			foreach(Component c in comps) {
				recv = c as IToggleReceiver;
				if (recv != null) {
					recv.Toggle();
				}
			}
		}
	}
}
