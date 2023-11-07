using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;

namespace Tortis.ChangeStream.ActiveMQ;

[PublicAPI]
[UsedImplicitly]
public sealed class AmqpPublisher : IDestination
{
    IConnection? _connection;
    string? _topicName;
    
    static readonly string CONNECTION_STRING_NAME = "ConnectionStringKey";
    static readonly string TOPIC_NAME = "TopicName";
    
    static readonly JsonSerializerOptions JSON_OPTIONS;

    static AmqpPublisher()
    {
        JSON_OPTIONS = new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } };
    }
    
    public void Configure(Dictionary<string, object> settings, IConnectionStringFactory connectionStringFactory, ILoggerFactory loggerFactory)
    {
        var connectionString = connectionStringFactory.GetConnectionString((string)settings[CONNECTION_STRING_NAME]);
        var factory = new ConnectionFactory(new Uri(connectionString));
        _connection = factory.CreateConnection();
        _connection.Start();
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

        using var channel = await _connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
        using var producer = await channel.CreateProducerAsync(new ActiveMQTopic(TOPIC_NAME));
        producer.DeliveryMode = MsgDeliveryMode.Persistent;

        await producer.SendAsync(new ActiveMQMessage
        {
            Content = buffer.ToArray()
        });
    }

    public void Dispose()
    {
        _connection?.Dispose();
    }
}