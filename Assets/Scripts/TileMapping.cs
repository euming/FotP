using UnityEngine;
using System.Collections;
[ExecuteInEditMode]

//	this just initializes the textures for the red tile
public class TileMapping : MonoBehaviour {

	public int row;
	public int col;

	public int maxRow = 4;
	public const int maxCol = 4;

	public Vector2 texOffset;

	public Material mat;
	public TileMapDatabase tileDB;

	void Awake() {
		mat = GetComponent<Renderer>().sharedMaterial;
		texOffset.x = (float)col / (float)maxCol;
		texOffset.y = (float)row / (float)maxRow;
	}

	// Use this for initialization
	void Start () {
	}

	//	do this shit only in the editor because this should be baked by the time we ship. but it's convenient to do it this way while in dev.
	void EditorOnlyUpdate()
	{
		this.gameObject.name = tileDB.GetName(row, col);
		texOffset.x = (float)col / (float)maxCol;
		texOffset.y = (float)row / (float)maxRow;
		mat.mainTextureOffset = texOffset;
	}

	// Update is called once per frame
	void Update () {
		EditorOnlyUpdate();
	}
}
