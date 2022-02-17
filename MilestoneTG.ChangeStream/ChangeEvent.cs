namespace MilestoneTG.ChangeStream;

public class ChangeEvent
{
    public Operation Operation { get; set; }
    public Dictionary<string, object> Values { get; set; } = new Dictionary<string, object>();
}