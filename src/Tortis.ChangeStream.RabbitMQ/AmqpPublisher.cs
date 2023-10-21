using System.Text.Json;
using System.Text.Json.Serialization;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Tortis.ChangeStream.RabbitMQ;

[PublicAPI]
[UsedImplicitly]
public sealed class AmqpPublisher : IDestination
{
    IConnection? _connection;
    string? _topicName;
    
    static readonly string CONNECTION_STRING_NAME = "ConnectionStringName";
    static readonly string TOPIC_NAME = "TopicName";
    
    static readonly JsonSerializerOptions JSON_OPTIONS;

    static AmqpPublisher()
    {
        JSON_OPTIONS = new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } };
    }
    
    public void Configure(Dictionary<string, object> settings, IConnectionStringFactory connectionStringFactory, ILoggerFactory loggerFactory)
    {
        var connectionString = connectionStringFactory.GetConnectionString((string)settings[CONNECTION_STRING_NAME]);
        var factory = new ConnectionFactory
        {
            Uri = new Uri(connectionString)
        };
        _connection = factory.CreateConnection();
        _topicName = (string)settings[TOPIC_NAME];
    }

    public async Task PublishAsync(ChangeEvent changeEvent, CancellationToken cancellationToken = default)
    {
        if (_connection is null)
            throw new InvalidOperationException("Publisher is not configured. Be sure to call Configure() before calling Publish()");
        
        using var buffer = new MemoryStream();
        await JsonSerializer.SerializeAsync(buffer, changeEvent, JSON_OPTIONS, cancellationToken);

        if (cancellationToken.IsCancellationRequested)
            return;
        
        using var channel = _connection.CreateModel();
        channel.BasicPublish(exchange: _topicName, routingKey: string.Empty, body: buffer.ToArray());
    }

    public void Dispose()
    {
        _connection?.Dispose();
    }
}