using System.Text.Json;
using System.Text.Json.Serialization;

namespace MilestoneTG.ChangeStream;

sealed class ConsoleDestination : IDestination
{
    static readonly JsonSerializerOptions _jsonOptions;

    static ConsoleDestination()
    {
        _jsonOptions = new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } };
    }

    public ConsoleDestination(Dictionary<string, object> settings, IConnectionStringFactory connectionStringFactory)
    {
    }

    public Task PublishAsync(ChangeEvent changeEvent)
    {
        //test error
        //throw new Exception("This is a test of the emergency broadcast system.");
        Console.WriteLine(JsonSerializer.Serialize(changeEvent, _jsonOptions));
        return Task.CompletedTask;
    }
}