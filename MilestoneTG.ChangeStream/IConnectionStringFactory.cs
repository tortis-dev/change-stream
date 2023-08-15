namespace MilestoneTG.ChangeStream;

public interface IConnectionStringFactory
{
    string GetConnectionString(string name);
}