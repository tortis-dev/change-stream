namespace MilestoneTG.ChangeStream;

public class ChangeEvent
{
    public Guid EventId { get; } = Guid.NewGuid();
    
    public DateTime Timestamp { get; set; }
    public Operation Operation { get; set; }
    public Dictionary<string, object> Values { get; } = new Dictionary<string, object>();
}