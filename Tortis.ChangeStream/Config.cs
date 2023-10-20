namespace Tortis.ChangeStream;

[PublicAPI]
public class CdcSettings
{
    public StreamSettings[] Streams { get; set; } = Array.Empty<StreamSettings>();
}

[PublicAPI]
public class StreamSettings
{
    public TimeSpan CircuitBreakerTimeout { get; set; } = TimeSpan.FromMinutes(5);
    public int CheckIntervalInMilliseconds { get; set; } = 1000;

    public SourceSettings Source { get; set; } = new();

    public DestinationSettings Destination { get; set; } = new();
}

[PublicAPI]
public class SourceSettings
{
    public string SourceName { get; set; } = string.Empty;
    public string SourceType { get; set; } = string.Empty;
    public Dictionary<string, object> Properties { get; } = new();
}

[PublicAPI]
public class DestinationSettings
{
    public string DestinationName { get; set; } = string.Empty;
    public string DestinationType { get; set; } = string.Empty;
    public Dictionary<string, object> Properties { get; } = new();
}