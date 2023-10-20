namespace Tortis.ChangeStream;

[PublicAPI]
public interface IConnectionStringFactory
{
    string GetConnectionString(string name);
}