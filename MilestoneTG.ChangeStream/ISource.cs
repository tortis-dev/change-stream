namespace MilestoneTG.ChangeStream;

[PublicAPI]
public interface ISource : IDisposable
{
    IAsyncEnumerable<ChangeEvent> GetChanges(CancellationToken cancellationToken);
}