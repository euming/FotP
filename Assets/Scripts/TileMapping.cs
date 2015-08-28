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
		//mat = GetComponent<Renderer>().material;	//	don't use shared material here because we want the instance to be different for each tile depending on tiling.

		Material tempMaterial = new Material(GetComponent<Renderer>().sharedMaterial);
		tempMaterial.name = GetComponent<Renderer>().sharedMaterial.name + " " + this.name;
		GetComponent<Renderer>().sharedMaterial = tempMaterial;
		texOffset.x = (float)col / (float)maxCol;
		texOffset.y = (float)row / (float)maxRow;
		mat = tempMaterial;
	}

	// Use this for initialization
	void Start () {
	}

	//	do this shit only in the editor because this should be baked by the time we ship. but it's convenient to do it this way while in dev.
	void EditorOnlyUpdate()
	{
		if (!tileDB) {
			Debug.LogWarning("Tile " + this.name + " has no tileDB.");
		}
		else {
			string newName = tileDB.GetName(row, col);
			if (newName != null) {
				this.gameObject.name = newName;
			}
		}
		texOffset.x = (float)col / (float)maxCol;
		texOffset.y = (float)row / (float)maxRow;
		mat.mainTextureOffset = texOffset;
	}

	// Update is called once per frame
	void Update () {
		EditorOnlyUpdate();
	}
}
