using UnityEngine;
using System.Collections;
using System.Collections.Generic;
[ExecuteInEditMode]

public class PositionToggler : ToggleReceiver {

	public List<Vector3> 			positions;
	public List<Quaternion> 		rotations;
	public int						curIndex;

	void Awake() {
		curIndex = 0;
		if (positions==null) {
			positions = new List<Vector3>();
		}
		if (rotations==null) {
			rotations = new List<Quaternion>();
		}
	}

	public void SetState(int idx)
	{
		if ((idx >= 0) && (idx < positions.Count)) {
			curIndex = idx;
			this.transform.localPosition = positions[idx];
			this.transform.localRotation = rotations[idx];
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

#endif
}
