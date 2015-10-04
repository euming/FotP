using UnityEngine;
using System.Collections;

public class UIState : MonoBehaviour {

    public UnityEngine.UI.Button buttonCancel;
    public UnityEngine.UI.Button buttonDone;
    static UIState s_instance;

    static public void EnableDoneButton(bool bActive=true)
    {
        s_instance.buttonDone.gameObject.SetActive(bActive);
    }
    static public void EnableCancelButton(bool bActive=true)
    {
        s_instance.buttonCancel.gameObject.SetActive(bActive);
    }

    public void OnDoneClick()
    {
        if (this.isActiveAndEnabled)
        {
            GameState.GetCurrentGameState().currentPlayer.OnDoneClick();
        }
    }
    public void OnCancelClick()
    {
        if (this.isActiveAndEnabled)
        {
            GameState.GetCurrentGameState().currentPlayer.OnCancelClick();
        }

    }

    void Awake()
    {
        if (s_instance == null)
        {
            s_instance = this;
        }
        else
        {
            Debug.LogError("Bad singleton with UIState");
        }
    }
    // Use this for initialization
    void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}
