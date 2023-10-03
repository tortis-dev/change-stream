using Microsoft.Extensions.Logging;

namespace MilestoneTG.ChangeStream;

[PublicAPI]
public interface IDestination : IDisposable
{
    void Configure(Dictionary<string, object> settings, IConnectionStringFactory connectionStringFactory, ILoggerFactory loggerFactory);
    
    Task PublishAsync(ChangeEvent changeEvent, CancellationToken cancellationToken);
}