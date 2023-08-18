using MilestoneTG.ChangeStream.SqlServer;

namespace MilestoneTG.ChangeStream.Server;

/// <summary>
/// Creates and manages Source/Destination instance pairs and their lifecycle.
/// Subscribes the destination to the source's IObservable.
/// </summary>
sealed class Propagator
{
    readonly SourceSettings _sourceSettings;
    readonly DestinationSettings _destinationSettings;
    readonly IServiceProvider _container;
    readonly ILogger<Propagator> _logger;

    IChangeSource? _source;
    IDestination? _destination;
    
    public Propagator(SourceSettings sourceSettings, DestinationSettings destinationSettings, IServiceProvider container)
    {
        _sourceSettings = sourceSettings;
        _destinationSettings = destinationSettings;
        _container = container;
        _logger = container.GetRequiredService<ILogger<Propagator>>();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            switch (_sourceSettings.SourceType.ToLowerInvariant())
            {
                case "sqlserver":
                {
                    var settings = new SqlServerChangeSourceSettings
                    {
                        IntervalInMilliseconds = _sourceSettings.IntervalInMilliseconds,
                        SchemaName = _sourceSettings.SchemaName,
                        TableName = _sourceSettings.TableName,
                        ConnectionString = _container.GetRequiredService<IConnectionStringFactory>().GetConnectionString(_sourceSettings.SourceName)
                    };
                    _source = new SqlServerChangeSource(settings,
                        _container.GetRequiredService<ILogger<SqlServerChangeSource>>());
                    break;
                }
                default:
                    _source = (IChangeSource)_container.GetRequiredService(Type.GetType(_sourceSettings.SourceType)!);
                    break;
            }
            
            switch (_destinationSettings.DestinationType.ToLowerInvariant())
            {
                case "console":
                    _destination = new ConsoleDestination();
                    break;
                default:
                    _destination = (IDestination)_container.GetRequiredService(Type.GetType(_destinationSettings.DestinationType)!);
                    break;
            }
           
            _source.ChangeStream.Subscribe(onNext: _destination.Publish, onError: OnError);
            _source.StartObserving();
        }
        catch (TaskCanceledException)
        {
            //expected. Graceful shutdown.
        }

        return Task.CompletedTask;
    }
    
    public Task StopAsync(CancellationToken cancellationToken)
    {
        _source?.StopObserving();
        return Task.CompletedTask;
    }

    void OnError(Exception exception)
    {
        _logger.LogError(exception, exception.Message);
    }
}
