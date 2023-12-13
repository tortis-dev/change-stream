using Microsoft.Extensions.Hosting;

namespace Tortis.ChangeStream;

sealed class PropagatorHostedService : IHostedService
{
    readonly CdcSettings _settings;
    readonly PropagatorFactory _factory;
    readonly Task[] _propagators;

    public PropagatorHostedService(CdcSettings settings, PropagatorFactory factory)
    {
        _settings = settings;
        _factory = factory;
        _propagators = new Task[_settings.Streams.Length];
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        for (var i=0; i < _settings.Streams.Length; i++)
        {
            var propagator = _factory.CreatePropagator(_settings.Streams[i]);
            _propagators[i] = Task.Factory.StartNew(
                () => propagator.ExecuteAsync(cancellationToken),
                TaskCreationOptions.LongRunning);
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        Task.WhenAll(_propagators);
        return Task.CompletedTask;
    }
}