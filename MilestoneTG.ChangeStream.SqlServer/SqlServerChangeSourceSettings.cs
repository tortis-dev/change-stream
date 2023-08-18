namespace MilestoneTG.ChangeStream.SqlServer;

public class SourceSettings
{
    public string ConnectionString { get; set; } = string.Empty;
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public int IntervalInMilliseconds { get; set; } = 1000;
}