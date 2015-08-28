using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class DiceFactory : MonoBehaviour {
	
	public enum DieType
	{
		Red,
		White,
		Black,
		Purple,
		Yellow,
		Green,
		Blue,
		Orange
	};

	public List<PharoahDie> 	prefabDice;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	public PharoahDie NewDie(DieType dieType)
	{
		PharoahDie newDie = GameObject.Instantiate(prefabDice[(int)dieType]);
		if (newDie.isSetDie()) {
			newDie.MoveToSetDieArea();
		}
		return newDie;
	}
}
