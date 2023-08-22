namespace MilestoneTG.ChangeStream;

public interface IDestination
{
    Task PublishAsync(ChangeEvent changeEvent);
}