using UnityEngine;
using System.Collections;
using UnityEditor;


//	This class lets you toggle between various positions and orientations
//	
[CustomEditor(typeof(DebugReporter))]
public class DebugReporterEditor : Editor
{
    public GameObject[] fullTileList = null;

    //  report the progress of Tiles
    public void ReportTileDevProgress()
    {
        DebugReporter myScript = (DebugReporter)target;
        GameObject[] inGameTileList;
        bool inGameOnly = myScript.inGameOnly;
        if (inGameOnly)
        {
            inGameTileList = GameObject.FindGameObjectsWithTag("Tile");
        }
        else
        {
            if (fullTileList == null)
            {
                //var list = AssetDatabase.LoadAssetAtPath("Assets/Prefabs/Tiles", typeof(GameObject));
                
                /*
                Tile[] tileList = AssetDatabase.LoadAssetAtPath("Assets/Prefabs/Tiles", Tile) as Tile[];
                    //GameObject.FindObjectsOfTypeIncludingAssets(typeof(Tile)) as Tile[];
                //fullTileList = new GameObject[tileList.Length];
                int idx = 0;
                foreach (Tile tile in tileList)
                {
                    fullTileList[idx] = tile.gameObject;
                    idx++;
                }
                */
            }
            //C:\Users\ming\Documents\UnityProjects\FotP\Assets\Prefabs\Tiles\Rank3
            //var list = AssetDatabase.LoadAllAssetsAtPath("Assets/Prefabs/Tiles/Rank3/");
            string tilePath = "FotPPrefabs/Tiles";
            Object[] list = Resources.LoadAll(tilePath, typeof(Tile));
            Debug.Log("Loaded " + list.Length.ToString() + " Tiles\n");
            inGameTileList = fullTileList;

            fullTileList = new GameObject[list.Length];
            int idx = 0;
            foreach (Tile tile in list)
            {
                //Debug.Log("prefab found: " + tile.name + "\n");
                fullTileList[idx] = tile.gameObject;
                idx++;
            }
            inGameTileList = fullTileList;

        }
        foreach (GameObject go in inGameTileList) {
            Tile tile = go.GetComponent<Tile>();
            string debugStatus = tile.GetDebugStatusString();
            Debug.Log(debugStatus + " " + go.name + "\n");
        }

    }
    public override void OnInspectorGUI()
    {
        DrawDefaultInspector();
        DebugReporter myScript = (DebugReporter)target;
        if (GUILayout.Button("Report Tile Dev Progress"))
        {
            ReportTileDevProgress();
        }
    }
}
