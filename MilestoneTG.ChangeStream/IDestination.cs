namespace MilestoneTG.ChangeStream;

[PublicAPI]
public interface IDestination : IDisposable
{
    Task PublishAsync(ChangeEvent changeEvent, CancellationToken cancellationToken);
}