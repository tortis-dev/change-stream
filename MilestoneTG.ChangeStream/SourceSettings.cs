namespace MilestoneTG.ChangeStream;

public class CdcSettings
{
    public StreamSettings[] Streams { get; set; }
}
public class StreamSettings
{
    public SourceSettings Source { get; set; }

    public DestinationSettings Destination { get; set; }
}

public class SourceSettings
{
    public string SourceName { get; set; } = string.Empty;
    public string SourceType { get; set; } = string.Empty;
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public int IntervalInMilliseconds { get; set; } = 1000;
}

public class DestinationSettings
{
    public string DestinationName { get; set; } = string.Empty;
    public string DestinationType { get; set; } = string.Empty;
    public string TopicPath { get; set; } = string.Empty;
}