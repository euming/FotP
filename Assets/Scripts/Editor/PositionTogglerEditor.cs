using UnityEngine;
using System.Collections;
using UnityEditor;


//	This class lets you toggle between various positions and orientations
//	
[CustomEditor(typeof(PositionToggler))]
public class PositionTogglerEditor : Editor
{
	//	sometimes, we just want to fix things and move on. This allows us to do that. But we should go back and fix the bugs for realz.
	const bool QUICKHACK_EDITS = false;

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

		//	iTween bug keeps creating iTween components in editor
		if (GUILayout.Button ("Remove itween components")) {
			myScript.RemoveITweenComponentsTree();
		}
#if QUICKHACK_EDITS
		//	hack: do this automatically
		myScript.RemoveITweenComponents();
#endif
#if false	//	one time edit hack
		if (GUILayout.Button ("Swap BarSlot PositionToggler 0 and 1")) {
			myScript.SwapBarSlotPositions();
		}
#endif
	}
}