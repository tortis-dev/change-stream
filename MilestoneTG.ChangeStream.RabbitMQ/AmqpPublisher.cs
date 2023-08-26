using System.Text.Json;
using System.Text.Json.Serialization;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace MilestoneTG.ChangeStream.RabbitMQ;

[PublicAPI]
[UsedImplicitly]
public sealed class AmqpPublisher : IDestination
{
    IConnection _connection;
    string _topicName;
    
    static readonly string ConnectionStringName = "ConnectionStringName";
    static readonly string TopicName = "TopicName";
    
    static readonly JsonSerializerOptions JsonOptions;

    static AmqpPublisher()
    {
        JsonOptions = new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } };
    }
    
    public void Configure(Dictionary<string, object> settings, IConnectionStringFactory connectionStringFactory, ILoggerFactory loggerFactory)
    {
        var connectionString = connectionStringFactory.GetConnectionString((string)settings[ConnectionStringName]);
        var factory = new ConnectionFactory
        {
            Uri = new Uri(connectionString)
        };
        _connection = factory.CreateConnection();
        _topicName = (string)settings[TopicName];
    }

    public async Task PublishAsync(ChangeEvent changeEvent, CancellationToken cancellationToken = default)
    {
        using var buffer = new MemoryStream();
        await JsonSerializer.SerializeAsync(buffer, changeEvent, JsonOptions, cancellationToken);

        if (cancellationToken.IsCancellationRequested)
            return;
        
        using var channel = _connection.CreateModel();
        channel.BasicPublish(exchange: _topicName, routingKey: string.Empty, body: buffer.ToArray());
    }

    public void Dispose()
    {
        _connection.Dispose();
    }
}