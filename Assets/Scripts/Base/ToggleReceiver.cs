using UnityEngine;
using System.Collections;

public class ToggleReceiver : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	//	your class needs to receive this toggle and do something with it.
	public virtual int Toggle()
	{
		int retval = 0;
		return retval;
	}
}
