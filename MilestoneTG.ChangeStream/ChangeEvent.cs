namespace MilestoneTG.ChangeStream;

/// <summary>
/// Represents a change to an entity from a change source.
/// </summary>
[PublicAPI]
public sealed class ChangeEvent
{
    /// <summary>
    /// This is a unique Id for the event instance as it appears on the event bus.
    /// </summary>
    public string EventId { get; set; } = string.Empty;

    /// <summary>
    /// The time the event was published to the event bus in UTC.
    /// </summary>
    public DateTime Timestamp { get; set; }
    
    /// <summary>
    /// The CUD operation that was performed on the entity.
    /// </summary>
    public Operation Operation { get; set; }
    
    /// <summary>
    /// The entire entity (record) tracked by underlying Change Data Capture implementation.
    /// </summary>
    public Dictionary<string, object> Entity { get; } = new ();

    /// <summary>
    /// The changed values. 
    /// </summary>
    public Dictionary<string, object> Changes { get; } = new();
}