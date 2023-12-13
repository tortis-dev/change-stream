using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Tortis.ChangeStream;

[PublicAPI]
public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddChangeStream(this IServiceCollection services, IConfiguration configuration)
    {
        services.UseStreamConnectionFactory<AppSettingsConnectionStringFactory>();
        services.AddSingleton<PropagatorFactory>();

        var settings = new CdcSettings();
        configuration.Bind("Cdc", settings);
        services.AddSingleton(settings);
        services.AddHostedService<PropagatorHostedService>();
        return services;
    }

    public static IServiceCollection UseStreamConnectionFactory<T>(this IServiceCollection services)
        where T : IConnectionStringFactory
    {
        services.Replace(ServiceDescriptor.Singleton(typeof(IConnectionStringFactory), typeof(T)));
        return services;
    }
}