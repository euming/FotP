﻿using UnityEngine;
using System.Collections;
using System.Collections.Generic;
[ExecuteInEditMode]

//	this maintains various states of position/orientation/scale
//	so that we may move things among the various states
public class PositionToggler : ToggleReceiver {

	public List<Vector3> 			positions;
	public List<Quaternion> 		rotations;
	public int						curIndex;	//	state
	public float					animTime = 0.8f;
	protected bool 	bUseTween = true;

	void Awake() {
		curIndex = 0;
		if (positions==null) {
			positions = new List<Vector3>();
		}
		if (rotations==null) {
			rotations = new List<Quaternion>();
		}
	}

	virtual public void SetState(int idx)
	{
		if ((idx >= 0) && (idx < positions.Count)) {
			curIndex = idx;
			if (bUseTween) {
				iTween.MoveToLocal(gameObject, positions[idx], animTime);
				iTween.RotateToLocal(gameObject, rotations[idx].eulerAngles, animTime);
			}
			else {
				this.transform.localPosition = positions[idx];
				this.transform.localRotation = rotations[idx];
			}
		}
	}

	public override int Toggle()
	{
		ToggleNext();
		return curIndex;
	}

	public void ToggleNext()
	{
		if (positions.Count == 0) return;
		
		curIndex++;
		if (curIndex >= positions.Count) {
			curIndex = 0;
		}
		SetState (curIndex);
	}

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

#if UNITY_EDITOR
	//	these things help us build our data in the editor
	public void SetKeyframe()
	{
		Vector3 newPos = new Vector3(
			this.transform.localPosition.x,
			this.transform.localPosition.y,
			this.transform.localPosition.z
			);
		positions.Add(newPos);
		Quaternion newRot = new Quaternion(
			this.transform.localRotation.x,
			this.transform.localRotation.y,
			this.transform.localRotation.z,
			this.transform.localRotation.w
			);
		rotations.Add(newRot);
	}

	public void AlignXpos()
	{
		int idx = 0;
		foreach(Vector3 pos in positions) {
			Vector3 newPos = new Vector3(this.transform.localPosition.x, pos.y, pos.z);
			positions[idx] = newPos;
			idx++;
		}
	}

	public void RemoveITweenComponents()
	{
		iTween[] components = this.GetComponents<iTween>();
		foreach(Component c in components) {
			if (c != null) {
				DestroyImmediate (c);
			}
		}
	}

	void Swap01()
	{
		Vector3 swapPos = positions[0];
		Quaternion swapQuat = rotations[0];
		
		positions[0] = positions[1];
		rotations[0] = rotations[1];
		positions[1] = swapPos;
		rotations[1] = swapQuat;
	}

	public void SwapBarSlotPositions()
	{
		Bar bar = this.GetComponent<Bar>();
		if (bar) {
			foreach(BarSlot slot in bar.barSlotList) {
				PositionToggler ptglr = slot.GetComponent<PositionToggler>();
				if (ptglr!=null) {
					ptglr.Swap01();
				}
			}
		}
	}

#endif
}
