namespace MilestoneTG.ChangeStream;

public enum Operation
{
    delete     = 1,
    create     = 2,
    update_old = 3,
    update_new = 4
}