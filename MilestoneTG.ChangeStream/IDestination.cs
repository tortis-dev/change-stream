namespace MilestoneTG.ChangeStream;

public interface IDestination
{
    void Publish(ChangeEvent changeEvent);
}