using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MilestoneTG.ChangeStream;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddChangeStream(this IServiceCollection services, IConfiguration configuration)
    {
        services.UseStreamConnectionFactory<AppSettingsConnectionStringFactory>();
        services.AddSingleton<PropagatorFactory>();

        var settings = new CdcSettings();
        configuration.Bind("cdc", settings);

        foreach (var stream in settings.Streams)
        {
            services.AddHostedService(container =>
            {
                var factory = container.GetRequiredService<PropagatorFactory>();
                return factory.CreatePropagator(stream);
            });
        }

        return services;
    }

    public static IServiceCollection UseStreamConnectionFactory<T>(this IServiceCollection services)
        where T : IConnectionStringFactory
    {
        services.Replace(ServiceDescriptor.Singleton(typeof(IConnectionStringFactory), typeof(T)));
        return services;
    }
}