using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;

namespace Tortis.ChangeStream;

/// <summary>
/// Creates and manages Source/Destination instance pairs and their lifecycle.
/// </summary>
sealed class Propagator : IDisposable
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
                onBreak: (exception, _) => _logger.LogWarning(exception, "Circuit Breaker tripped."),
                onReset: () => { })
            .WrapAsync(Policy.Handle<Exception>().WaitAndRetryAsync(new[] { TimeSpan.FromMilliseconds(100) }));
    }
    
    internal async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting Propagator for source {source}", _source.GetType().FullName);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _policy.ExecuteAsync(async token =>
                {
                    try
                    {
                        await foreach (var change in _source.GetChanges(token))
                            await _destination.PublishAsync(change, token);
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

    public void Dispose()
    {
        _source.Dispose();
        _destination.Dispose();
    }
}
