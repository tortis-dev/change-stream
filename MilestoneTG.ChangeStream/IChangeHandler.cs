namespace MilestoneTG.ChangeStream;

public interface IChangeHandler
{
    void Handle(ChangeEvent changeEvent);
}