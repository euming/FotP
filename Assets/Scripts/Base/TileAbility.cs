using UnityEngine;
using System.Collections;

//	this is what grants the tile some ability.
public class TileAbility : MonoBehaviour {

	public bool isArtifact;
	public bool isArtifactUsed;		//	Artifacts may be used once per game. Once used, we can't use it again
	public bool isUsedThisTurn;		//	true if we already used this ability this turn

	/*
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
	*/

	//	does something when we acquire this tile
	public virtual void OnAcquire(PlayerBoard plr)
	{
	}

	//	if we change our mind and undo the acquire
	public virtual void OnAcquireUndo(PlayerBoard plr)
	{
	}

	//	does something when we select this tile
	public virtual void OnSelect(PlayerBoard plr)
	{
		isUsedThisTurn = true;
		if (isArtifact)
			isArtifactUsed = true;
	}
}
