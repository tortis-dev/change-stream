using Microsoft.Data.SqlClient;
using System.Reactive.Subjects;
using System.Text.Json;
using System.Text.Json.Serialization;
using MilestoneTG.ChangeStream.Server.SqlServer;

namespace MilestoneTG.ChangeStream.Server;

public class Propagator : IHostedService
{
    readonly ILoggerFactory _loggerFactory;

    public Propagator(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            var source = new SqlServerChangeSource(cancellationToken, _loggerFactory);
            var jsonOptions = new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } };

            source.Subscribe(changeEvent => Console.WriteLine(JsonSerializer.Serialize(changeEvent, jsonOptions)));

            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        catch (TaskCanceledException)
        {
            //expected. Graceful shutdown.
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}