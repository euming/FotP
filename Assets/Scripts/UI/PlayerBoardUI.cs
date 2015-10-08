using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class PlayerBoardUI : MonoBehaviour {

    public TileSlot     slotPrefab;
    public Tile         tilePrefab;
    private List<TileSlot> slotList;

    public int rows;
    public int cols;
    public float space_width;   //  space between tiles
    public float space_height;

    void Awake()
    {
        slotList = new List<TileSlot>();
    }
	// Use this for initialization
	void Start () {
        GameState.Message("PlayerBoardUI.Start()");
        float xpos, ypos, zpos, deltax, deltay;
        Collider col = tilePrefab.GetComponent<Collider>();
        deltax = col.bounds.size.x + space_width;
        deltay = col.bounds.size.y + space_height;
        ypos = 0.0f;
        zpos = 0.0f;
        for (int y = 0; y < cols; y++)
        {
            xpos = 0.0f;
            for (int x = 0; x < rows; x++)
            {
                GameObject slotInst = Instantiate(slotPrefab.gameObject);
                slotInst.transform.parent = this.transform;
                slotInst.transform.localPosition = new Vector3(xpos, ypos, zpos);
                TileSlot ts = slotInst.GetComponent<TileSlot>();
                slotList.Add(ts);
                xpos += deltax;
            }
            ypos += deltay;
        }
	}

    TileSlot GetNextEmptySlot()
    {
        foreach(TileSlot ts in slotList)
        {
            if (ts.isEmpty()) return ts;
        }
        return null;
    }
    public void AddTile(Tile tile)
    {
        TileSlot ts = GetNextEmptySlot();
        ts.addChild(tile.gameObject);
    }
    public void RemoveTile(Tile tile)
    {
        TileSlot tsParent = tile.transform.parent.GetComponent<TileSlot>();
        tsParent.removeChild(tile.gameObject);
    }
	// Update is called once per frame
	void Update () {
	
	}
}
