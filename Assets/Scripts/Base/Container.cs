using UnityEngine;
using System.Collections;

//	A container is an abstract concept. No physical things should happen here. Use Slot for physical things.
[System.Serializable]
public class Container : MonoBehaviour {

	public GameObject		myChild;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	}

	void OnMouseDown() {
		Debug.Log("Container.OnMouseDown()");
		GameState gs = GameState.GetCurrentGameState();
		PlayerBoard currentPlayer = gs.currentPlayer;
		OnSelect(currentPlayer);
	}

	public bool isEmpty()
	{
		if (myChild==null) return true;
		return false;
	}

	public GameObject WhatsInTheBox()
	{
		return myChild;
	}

	public virtual bool addChild(GameObject child)
	{
		bool bSuccess = false;
		if (myChild!=null) {
			removeChild (myChild);
		}
		myChild = child;
		child.gameObject.transform.parent = this.gameObject.transform;
		OnAddChild(child);
		return bSuccess;
	}

	public virtual GameObject removeChild(GameObject child)
	{
		if (child == myChild) {
			return removeChild();
		}
		return null;
	}
	public virtual GameObject removeChild()
	{
		GameObject bHasRemovedChild = null;
		if (myChild != null) {
			bHasRemovedChild = myChild;
			OnRemoveChild (myChild);
			myChild.gameObject.transform.parent = null;
			myChild = null;
		}
		return bHasRemovedChild;
	}

	//	returns what was previously in the container, if it was removed to make place for the new child.
	public virtual GameObject OnAddChild(GameObject child)
	{
		Debug.Log("Container.OnAddChild():" + this.gameObject.name + " adds " + child.name);
		return null;
	}

	public virtual void OnRemoveChild(GameObject child)
	{
		Debug.Log("Container.OnRemoveChild():" + this.gameObject.name);
	}
	
	public virtual void OnSelect(PlayerBoard currentPlayer) {
		Debug.Log("Container.OnSelect():" + this.gameObject.name + " by: " + currentPlayer.name);
	}
}
