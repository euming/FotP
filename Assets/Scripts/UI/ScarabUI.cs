using UnityEngine;
using System.Collections;

public class ScarabUI : MonoBehaviour {
    public enum ScarabUIState
    {
        ready,              //  ready to be clicked in normal UI state
        scarab_activated,   //  we're going to try and use this scarab to do something
        waiting_die_select, //  we're waiting for the player to select a die to use the scarab on
        die_selected,       //  aha, the player has chosen a die
        ask_confirm,        //  ask the player to confirm
        consume_scarab,     //  after the player has chosen a die, destroy the player's scarab
        cleanup,            //  after everything is done, cleanup anything before return to ready state and then goto ready state
        err_unavail_scarab, //  no scarabs of this type available
    }
    public Scarab.ScarabType    type;
    public ScarabUIState        scuiState;
    public PharoahDie           selectedDie;
    public UnityEngine.UI.Button    buttonCancel;
    public UnityEngine.UI.Button    buttonDone;

    // Use this for initialization
    void Start () {
        SetState(ScarabUIState.ready);
        EnableButtons(false);
    }
	
	// Update is called once per frame
	void Update () {
	
	}

    void EnableButtons(bool bEnable)
    {
        buttonCancel.gameObject.SetActive(bEnable);
        buttonDone.gameObject.SetActive(bEnable);
    }

    bool SetState(ScarabUIState newState)
    {
        bool bSuccess = false;
        switch (newState)
        {
            default:
            case ScarabUIState.ready:
                selectedDie = null;
                bSuccess = true;
                break;

            //  try to use the scarab
            case ScarabUIState.scarab_activated:
                //  set aside the scarab for use
                if (GameState.GetCurrentGameState().currentPlayer.UseScarab(type)) {
                    if (SetState(ScarabUIState.waiting_die_select))
                    {
                        bSuccess = true;
                        newState = ScarabUIState.waiting_die_select;
                    }
                }
                break;
            case ScarabUIState.waiting_die_select:
                //GameState.GetCurrentGameState().currentPlayer.AskToChooseDie(this.type.ToString());
                //GameState.Message(this.name + " please select a die for scarab action");
                bSuccess = true;
                break;
        }
        if (bSuccess)
        {
            scuiState = newState;
        }
        return bSuccess;
    }

    void OnMouseDown()
    {
        Debug.Log("ScarabUI.OnMouseDown()");
        GameState gs = GameState.GetCurrentGameState();
        PlayerBoard currentPlayer = gs.currentPlayer;
        if (GameState.GetCurrentGameState().CheatModeEnabled)
        {
            currentPlayer.AddScarab(type);
        }
        switch (scuiState)
        {
            default:
                break;
            case ScarabUIState.ready:
                if (currentPlayer.hasScarab(type))
                {
                    SetState(ScarabUIState.scarab_activated);
                }
                else
                {
                    GameState.Message(currentPlayer.name + " does not have scarab " + type.ToString());
                }
                break;
            case ScarabUIState.waiting_die_select:
                GameState.Message(currentPlayer.name + " please select a die for scarab action");
                break;
        }

        //if (currentPlayer.UseScarab(type))
        //{
        //}
    }
}
