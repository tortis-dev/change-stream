namespace MilestoneTG.ChangeStream;

public enum Operation
{
    delete     = 1,
    insert     = 2,
    update_old = 3,
    update     = 4
}