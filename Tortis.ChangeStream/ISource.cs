using Microsoft.Extensions.Logging;

namespace Tortis.ChangeStream;

[PublicAPI]
public interface ISource : IDisposable
{
    void Configure(Dictionary<string, object> settings, IConnectionStringFactory connectionStringFactory, ILoggerFactory loggerFactory);
    
    IAsyncEnumerable<ChangeEvent> GetChanges(CancellationToken cancellationToken);
}