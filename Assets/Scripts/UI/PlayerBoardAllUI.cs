using UnityEngine;
using System.Collections;

public class PlayerBoardAllUI : MonoBehaviour, IToggleCallback
{
    public PlayerBoardAllUIState curState;
    PositionToggler posToggler;

    public enum PlayerBoardAllUIState
    {
        isTuckedAway = 0,   //	hidden at the top
                            //isCollapsed,	//	only relevant things can be seen
        isExpanded,         //	everything can be seen
        numOfPlayerBoardAllUIStates,
    };

    void Awake()
    {
        posToggler = GetComponent<PositionToggler>();
        posToggler.onToggleRecvrs.Add(this);
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
                //	need to put any loose dice in the active dice area
                GameState.GetCurrentGameState().currentPlayer.CollectLooseDice();
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
