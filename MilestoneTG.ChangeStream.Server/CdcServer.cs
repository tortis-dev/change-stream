using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace MilestoneTG.ChangeStream.Server;

class CdcServer : IHostedService
{
    readonly IOptionsMonitor<CdcSettings> _settings;
    readonly IServiceProvider _container;
    readonly ConcurrentBag<Propagator> _propagators = new();

    public CdcServer(IOptionsMonitor<CdcSettings> settings, IServiceProvider container)
    {
        _settings = settings;
        _container = container;
    }
    
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var streamSettings in _settings.CurrentValue.Streams)
        {
            var propagator = new Propagator(streamSettings.Source, streamSettings.Destination, _container);
            _propagators.Add(propagator);
            await propagator.StartAsync(cancellationToken);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var propagator in _propagators)
        {
            await propagator.StopAsync(cancellationToken);
        }
    }
}