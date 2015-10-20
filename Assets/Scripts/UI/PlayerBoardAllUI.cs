using UnityEngine;
using System.Collections;
using System.Collections.Generic;

//	when some UI needs to be refreshed.
public interface IRefreshUI
{
    void RefreshUI();
}

public class PlayerBoardAllUI : MonoBehaviour, IToggleCallback
{
    static private PlayerBoardAllUI instance = null;
    public PlayerBoardAllUIState curState;
    public List<ScarabUI> scarabUIs;

    PositionToggler posToggler;

    public enum PlayerBoardAllUIState
    {
        isTuckedAway = 0,   //	hidden at the top
                            //isCollapsed,	//	only relevant things can be seen
        isExpanded,         //	everything can be seen
        numOfPlayerBoardAllUIStates,
    };

    static public PlayerBoardAllUI getInstance()
    {
        return instance;
    }

    static public void RefreshNewPlayer()
    {
        RefreshScarabUI();
    }

    static public void RefreshScarabUI()
    {
        if (instance != null)
            instance.refreshScarabUI();
    }

    void refreshScarabUI()
    {
        foreach (ScarabUI ui in scarabUIs)
        {
            ui.RefreshUI();
        }
    }

    void Awake()
    {
        if (instance == null)
            instance = this;
        else
            Debug.LogError("ERROR: Only one instance of PlayerBoardAllUI is allowed.");
    }

    void Start()
    {
        posToggler = GetComponent<PositionToggler>();
        posToggler.AddOnToggleCallback(this);
    }

    public int SetState(PlayerBoardAllUIState newState)
    {
        curState = newState;
        posToggler.SetState((int)newState);
        switch (curState)
        {
            case PlayerBoardAllUIState.isTuckedAway:
                break;
            case PlayerBoardAllUIState.isExpanded:
                break;
        }
        return (int)newState;
    }
    //	when tapped, this does something
    public int ChangeState()
    {
        curState++;
        if (curState >= PlayerBoardAllUIState.numOfPlayerBoardAllUIStates)
        {
            curState = 0;
        }
        SetState(curState);
        return (int)curState;
    }

    public void OnToggle(int curIndex)
    {
        curState = (PlayerBoardAllUIState)curIndex;
        SetState(curState);
    }

}
