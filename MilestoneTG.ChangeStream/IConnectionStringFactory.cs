namespace MilestoneTG.ChangeStream;

[PublicAPI]
public interface IConnectionStringFactory
{
    string GetConnectionString(string name);
}