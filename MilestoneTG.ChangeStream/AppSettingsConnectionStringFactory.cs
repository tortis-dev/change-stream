using Microsoft.Extensions.Configuration;

namespace MilestoneTG.ChangeStream;

[UsedImplicitly]
sealed class AppSettingsConnectionStringFactory : IConnectionStringFactory
{
    readonly IConfiguration _configuration;

    public AppSettingsConnectionStringFactory(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public string GetConnectionString(string name)
    {
        return _configuration.GetConnectionString(name);
    }
}