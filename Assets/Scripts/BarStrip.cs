using UnityEngine;
using System.Collections;
[ExecuteInEditMode]
public class BarStrip : MonoBehaviour {

	public int row;
	public int col;
	
	public const int maxRow = 10;
	public const int maxCol = 1;
	
	public Vector2 texOffset;
	
	public Material mat;

	void Awake() {
		mat = GetComponent<Renderer>().sharedMaterial;
		texOffset.x = (float)col / (float)maxCol;
		texOffset.y = (float)row / (float)maxRow;
	}

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		texOffset.x = (float)col / (float)maxCol;
		texOffset.y = (float)row / (float)maxRow;
		mat.mainTextureOffset = texOffset;
	}
}
