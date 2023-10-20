using System.Reflection;
using Microsoft.Extensions.Logging;

namespace Tortis.ChangeStream;

sealed class PropagatorFactory
{
    readonly IConnectionStringFactory _connectionStringFactory;
    readonly ILoggerFactory _loggerFactory;
    readonly ILogger<Propagator> _logger;

    public PropagatorFactory(IConnectionStringFactory connectionStringFactory, ILoggerFactory loggerFactory)
    {
        _connectionStringFactory = connectionStringFactory;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<Propagator>();
    }

    public Propagator CreatePropagator(StreamSettings streamSettings)
    {
        var sourceParts = streamSettings.Source.SourceType.Split(',');
        var sourceType = Assembly.Load(sourceParts[1]).GetType(sourceParts[0]);

        if (sourceType is null)
            throw new TypeLoadException($"Change source implemented by type {streamSettings.Source.SourceType} not found.");

        if (Activator.CreateInstance(sourceType) is not ISource source)
            throw new TypeLoadException($"Unable to create instance of type {streamSettings.Source.SourceType}.");

        try
        {
            source.Configure(streamSettings.Source.Properties, _connectionStringFactory, _loggerFactory);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred while configuring the change source for {source}", streamSettings.Source.SourceName);
        }
        
        var destinationParts = streamSettings.Destination.DestinationType.Split(',');
        var destinationType = Assembly.Load(destinationParts[1]).GetType(destinationParts[0]);
        if (destinationType is null)
            throw new TypeLoadException($"Publish destination implemented by type {streamSettings.Destination.DestinationType} not found.");

        if (Activator.CreateInstance(destinationType) is not IDestination destination)
            throw new TypeLoadException($"Unable to create instance of type {streamSettings.Destination.DestinationType}.");

        try
        {
            destination.Configure(streamSettings.Destination.Properties, _connectionStringFactory, _loggerFactory);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred while configuring the publish destination for {destination}", streamSettings.Destination.DestinationName);
        }
        
        return new Propagator(source, destination, streamSettings, _logger);
    }
}