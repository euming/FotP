using UnityEngine;
using System.Collections;
using UnityEditor;

//	This class lets you toggle between various positions and orientations
//	
[CustomEditor(typeof(PositionToggler))]
public class PositionTogglerEditor : Editor
{
	public override void OnInspectorGUI()
	{
		DrawDefaultInspector();
		
		PositionToggler myScript = (PositionToggler)target;
		if(GUILayout.Button("Set Keyframe"))
		{
			myScript.SetKeyframe();
		}

		if(GUILayout.Button("Align xpos"))
		{
			myScript.AlignXpos();
		}

		if(GUILayout.Button("Test Toggle"))
		{
			myScript.ToggleNext();
		}
	}
}