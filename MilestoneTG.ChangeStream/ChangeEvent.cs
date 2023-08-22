namespace MilestoneTG.ChangeStream;

/// <summary>
/// Represents a change to an entity from a change source.
/// </summary>
public class ChangeEvent
{
    /// <summary>
    /// This is a unique Id for the event instance as it appears on the event bus.
    /// </summary>
    public Guid EventId { get; } = Guid.NewGuid();

    /// <summary>
    /// The Change Id as seen in the change source.
    /// For example, in SQL Server, this is the GUID representation of the Log Sequence Number (transaction id).
    /// </summary>
    public Guid ChangeId { get; set; }

    /// <summary>
    /// The sequence id of the operation within the change.
    /// For Example, in SQL Server, if multiple DML operations are performed in a single transaction,
    /// this is the sequence number within the transaction.
    /// </summary>
    public Guid OperationSequence { get; set; }
    
    /// <summary>
    /// The time the event was published to the event bus in UTC.
    /// </summary>
    public DateTime Timestamp { get; set; }
    
    /// <summary>
    /// The CUD operation that was performed on the entity.
    /// </summary>
    public Operation Operation { get; set; }
    
    /// <summary>
    /// The new values as tracked by underlying Change Data Capture implementation.
    /// </summary>
    public Dictionary<string, object> Values { get; } = new ();
}