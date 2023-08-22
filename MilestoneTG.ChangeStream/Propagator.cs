using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;

namespace MilestoneTG.ChangeStream;

/// <summary>
/// Creates and manages Source/Destination instance pairs and their lifecycle.
/// </summary>
sealed class Propagator : BackgroundService
{
    readonly ISource _source;
    readonly IDestination _destination;
    readonly StreamSettings _streamSettings;
    readonly ILogger<Propagator> _logger;
    readonly AsyncPolicy _policy;

    public Propagator(ISource source, IDestination destination, StreamSettings streamSettings, ILogger<Propagator> logger)
    {
        _source = source;
        _destination = destination;
        _streamSettings = streamSettings;
        _logger = logger;

        _policy = Policy.Handle<Exception>()
            .CircuitBreakerAsync(
                1,
                streamSettings.CircuitBreakerTimeout,
                onBreak: (exception, span) => _logger.LogWarning(exception, "Circuit Breaker tripped."),
                onReset: () => { })
            .WrapAsync(Policy.Handle<Exception>().WaitAndRetryAsync(new[] { TimeSpan.FromMilliseconds(100) }));
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _policy.ExecuteAsync(async token =>
                {
                    try
                    {
                        await foreach (var change in _source.GetChanges(stoppingToken))
                            await _destination.PublishAsync(change);
                    }
                    catch (TaskCanceledException)
                    {
                        // Expected. The service is stopping.
                        _logger.LogInformation("Stopping Propagator for source {source}", _source.GetType().FullName);
                    }
                }, stoppingToken);
            }
            catch (BrokenCircuitException)
            {
                // Expected. The circuit breaker is tripped.
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred. See exception for details.");
            }

            await Task.Delay(_streamSettings.CheckIntervalInMilliseconds, stoppingToken);
        }
    }
}
