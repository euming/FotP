using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class AddPips : TileAbility
{
    public int nPips = 1;
    public bool isExactlyNumPips = false;
    public int nDice = 1;   //  -1 for any number of dice.
    int actualNumDice;      //  number of dice we're allowed to modify
    public DieType onlyFor;
    PharoahDie curDie;
    PlayerBoard myPlayer;

    List<PharoahDie> adjustedDice;

    // Use this for initialization
    void Start () {
        adjustedDice = new List<PharoahDie>();
    }

    public override void OnStartTurn(PlayerBoard plr)
    {
        base.OnStartTurn(plr);
        this.isUsedThisTurn = false;    //  refresh this every turn.
    }

    public override void OnSelect(PlayerBoard plr)
    {
        base.OnSelect(plr);
        if (this.isUsedThisTurn)
        {
            GameState.Message("Already used " + this.name + " this turn.");
            return;
        }

        curDie = null;
        adjustedDice.Clear();
        if (nDice == -1)
        {
            actualNumDice = plr.GetNumValidDice(onlyFor);
        }
        else
        {
            actualNumDice = nDice;
        }
        myPlayer = plr;
        plr.SetTileInUse(this.GetComponent<Tile>());
        plr.AskToChooseDie(this.PickDie, this.GetType().ToString()); //  ask the player to choose a die or dice
        plr.AskToChooseCancel(this.OnCancel);
        plr.AskToChooseDone(this.OnDone);
    }

    public override void OnChooseDie(PlayerBoard plr)
    {
        base.OnChooseDie(plr);
    }

    void UndoPips(PharoahDie die)
    {
        die.UndoTempPips();
    }

    bool isNewDie(PharoahDie die)
    {
        return (!adjustedDice.Contains(die));
    }

    void OnCancel(PharoahDie d)
    {
        GameState.Message("Cancel");
        foreach(PharoahDie die in adjustedDice)
        {
            die.UndoTempPips();
        }
        myPlayer.UndoState();   //  go back to previous state
        UIState.EnableCancelButton(false);
        UIState.EnableDoneButton(false);
    }
    void OnDone(PharoahDie d)
    {
        GameState.Message("Done");
        foreach (PharoahDie die in adjustedDice)
        {
            die.FinalizeTempPips();
        }
        myPlayer.UndoState();   //  go back to previous state
        UIState.EnableCancelButton(false);
        UIState.EnableDoneButton(false);
        this.isUsedThisTurn = true;
    }
    //  delegate: when the player chooses a die, this will get called.
    //  user clicked on a die. Which one is it? We have to keep track here for this ability.
    void PickDie(PharoahDie die)
    {
        bool bLegalDie = false;
        bool bIsNewDie = false;

        curDie = die;

        if (isNewDie(die))
        {
            bIsNewDie = true;
        }
        else
        {
            bLegalDie = true;   //  we picked a die that we already have picked
        }

        if (adjustedDice.Count < actualNumDice)  //  can still pick new dice
        {
            if (bIsNewDie)
            {
                die.ClearTempPips();
                adjustedDice.Add(die);          //  add the new die to the list of dice we are modifying
                bLegalDie = true;
            }
        }

        //  if we are a legal die, then we can add pips to it (or undo the addpips)
        if (bLegalDie)
        {
            if (isExactlyNumPips)   //  we add exactly this number of pips.
            {
                if (die.getTempPips()==0)   //  we haven't messed with this die yet
                {
                    if (die.value + nPips > 6)  //  failure case. we can't add this many pips!
                    {
                        GameState.Message("ERROR: Can't add exactly " + nPips.ToString() + " to " + die.name);
                        adjustedDice.Remove(die);
                        return;
                    }
                    else
                    {
                        die.AddTempPips(nPips);
                    }
                }
                else//  we've messed with this die already
                {
                    die.UndoTempPips();
                }
            }
            else//  we can add up to the number of pips specified in nPips to any die
            {
                if (die.getTempPips()+1 > nPips)
                {
                    die.UndoTempPips();
                }
                else
                {
                    if (die.value + 1 > 6)  //  failure case. we can't add this many pips!
                    {
                        die.UndoTempPips();
                    }
                    die.AddTempPips(1);
                }
            }
        }
        else
        {
            GameState.Message("Can't choose " + die.name + " for " + this.name);
        }
        myPlayer.AskToChooseDie(this.PickDie, this.GetType().ToString()); //  ask the player to choose a die or dice
    }
}
