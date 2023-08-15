using System.Text.Json;
using System.Text.Json.Serialization;

namespace MilestoneTG.ChangeStream;

public class ConsoleDestination : IDestination
{
    static readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } };

    static ConsoleDestination()
    {
    }

    public void Publish(ChangeEvent changeEvent)
    {
        Console.WriteLine(JsonSerializer.Serialize(changeEvent, _jsonOptions));
    }
}