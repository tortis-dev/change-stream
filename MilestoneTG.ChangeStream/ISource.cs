namespace MilestoneTG.ChangeStream;

public interface ISource
{
    IAsyncEnumerable<ChangeEvent> GetChanges(CancellationToken cancellationToken);
}