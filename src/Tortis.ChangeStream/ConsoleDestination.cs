using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;

namespace Tortis.ChangeStream;

[UsedImplicitly]
public sealed class ConsoleDestination : IDestination
{
    static readonly JsonSerializerOptions JSON_OPTIONS;

    static ConsoleDestination()
    {
        JSON_OPTIONS = new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } };
    }

    public void Configure(Dictionary<string, object> settings, IConnectionStringFactory connectionStringFactory, ILoggerFactory loggerFactory)
    {
        
    }

    public Task PublishAsync(ChangeEvent changeEvent, CancellationToken cancellationToken)
    {
        Console.WriteLine(JsonSerializer.Serialize(changeEvent, JSON_OPTIONS));
        return Task.CompletedTask;
    }

    public void Dispose()
    {
    }
}