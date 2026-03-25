using FotP.Engine.Core;
using FotP.Engine.Dice;
using FotP.Engine.State;

namespace FotP.Engine.Tests.State;

public class DiceZoneManagerTests
{
    private static SmartList<Die> MakePool(int count, DieType type = DieType.Standard)
    {
        var pool = new SmartList<Die>();
        for (int i = 0; i < count; i++)
            pool.Add(new Die(type));
        return pool;
    }

    [Fact]
    public void CollectAllToCup_MovesAllDiceToCup()
    {
        var mgr = new DiceZoneManager();
        var pool = MakePool(3);
        mgr.CollectAllToCup(pool);
        Assert.Equal(3, mgr.Cup.Count);
        Assert.Equal(0, mgr.Active.Count);
        Assert.Equal(0, mgr.Locked.Count);
    }

    [Fact]
    public void RollAllInCup_MovesToActive()
    {
        var mgr = new DiceZoneManager();
        var pool = MakePool(3);
        mgr.CollectAllToCup(pool);
        mgr.RollAllInCup(new Random(1));
        Assert.Equal(0, mgr.Cup.Count);
        Assert.Equal(3, mgr.Active.Count);
    }

    [Fact]
    public void LockDie_MovesFromActiveToLocked()
    {
        var mgr = new DiceZoneManager();
        var pool = MakePool(2);
        mgr.CollectAllToCup(pool);
        mgr.RollAllInCup(new Random(1));
        var die = mgr.Active.First();
        mgr.LockDie(die);
        Assert.True(die.IsLocked);
        Assert.Contains(die, mgr.Locked);
        Assert.DoesNotContain(die, mgr.Active);
    }

    [Fact]
    public void LockDie_NotInActive_Throws()
    {
        var mgr = new DiceZoneManager();
        var die = new Die(DieType.Standard);
        Assert.Throws<InvalidOperationException>(() => mgr.LockDie(die));
    }

    [Fact]
    public void AutoLockImmediateDice_LocksOnlyImmediateDice()
    {
        var mgr = new DiceZoneManager();
        var pool = new SmartList<Die>();
        pool.Add(new Die(DieType.Immediate));
        pool.Add(new Die(DieType.Standard));
        mgr.CollectAllToCup(pool);
        mgr.RollAllInCup(new Random(1));
        var autoLocked = mgr.AutoLockImmediateDice();
        Assert.Single(autoLocked);
        Assert.Equal(DieType.Immediate, autoLocked[0].DieType);
        Assert.Equal(1, mgr.Active.Count);
    }

    [Fact]
    public void ReturnActiveToCup_MovesAllActiveBackToCup()
    {
        var mgr = new DiceZoneManager();
        var pool = MakePool(3);
        mgr.CollectAllToCup(pool);
        mgr.RollAllInCup(new Random(1));
        mgr.ReturnActiveToCup();
        Assert.Equal(3, mgr.Cup.Count);
        Assert.Equal(0, mgr.Active.Count);
    }

    [Fact]
    public void CollectAllToCup_ClearsLockedAndResets()
    {
        var mgr = new DiceZoneManager();
        var pool = MakePool(3);
        mgr.CollectAllToCup(pool);
        mgr.RollAllInCup(new Random(1));
        var die = mgr.Active.First();
        mgr.LockDie(die);
        Assert.Equal(1, mgr.Locked.Count);

        // Second collect resets everything
        mgr.CollectAllToCup(pool);
        Assert.Equal(3, mgr.Cup.Count);
        Assert.Equal(0, mgr.Locked.Count);
        Assert.False(die.IsLocked);
    }
}
