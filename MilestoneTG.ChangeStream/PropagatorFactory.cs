using System.Reflection;
using Microsoft.Extensions.Logging;

namespace MilestoneTG.ChangeStream;

sealed class PropagatorFactory
{
    readonly IConnectionStringFactory _connectionStringFactory;
    readonly ILogger<Propagator> _logger;

    public PropagatorFactory(IConnectionStringFactory connectionStringFactory, ILogger<Propagator> logger)
    {
        _connectionStringFactory = connectionStringFactory;
        _logger = logger;
    }

    public Propagator CreatePropagator(StreamSettings streamSettings)
    {
        var sourceParts = streamSettings.Source.SourceType.Split(',');
        var sourceType = Assembly.Load(sourceParts[1]).GetType(sourceParts[0]);
        var source = (ISource)Activator.CreateInstance(sourceType, args: new object[]{streamSettings.Source.Properties, _connectionStringFactory});
        
        var destinationParts = streamSettings.Destination.DestinationType.Split(',');
        var destinationType = Assembly.Load(destinationParts[1]).GetType(destinationParts[0]);
        var destination = (IDestination)Activator.CreateInstance(destinationType, args: new object[]{streamSettings.Destination.Properties, _connectionStringFactory});

        return new Propagator(source, destination, streamSettings, _logger);
    }
}