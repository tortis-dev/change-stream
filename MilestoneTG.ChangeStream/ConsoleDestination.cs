using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;

namespace MilestoneTG.ChangeStream;

[UsedImplicitly]
public sealed class ConsoleDestination : IDestination
{
    static readonly JsonSerializerOptions JsonOptions;

    static ConsoleDestination()
    {
        JsonOptions = new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } };
    }

    public ConsoleDestination(Dictionary<string, object> settings, IConnectionStringFactory connectionStringFactory, ILoggerFactory loggerFactory)
    {
    }

    public Task PublishAsync(ChangeEvent changeEvent, CancellationToken cancellationToken)
    {
        Console.WriteLine(JsonSerializer.Serialize(changeEvent, JsonOptions));
        return Task.CompletedTask;
    }

    public void Dispose()
    {
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}