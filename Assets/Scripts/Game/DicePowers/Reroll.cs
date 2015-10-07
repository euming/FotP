using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Reroll : TileAbility
{
    PlayerBoard myPlayer;

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
        myPlayer = plr;
        plr.SetTileInUse(this.GetComponent<Tile>());
        plr.AskToChooseDie(this.PickDie, this.GetType().ToString()); //  ask the player to choose a die or dice
    }
    //  delegate: when the player chooses a die, this will get called.
    //  user clicked on a die. Which one is it? We have to keep track here for this ability.
    bool PickDie(PharoahDie die)
    {
        return myPlayer.RollDie(die);
    }
}
