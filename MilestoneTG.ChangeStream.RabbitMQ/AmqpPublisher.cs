using System.Text.Json;
using System.Text.Json.Serialization;
using Polly;
using RabbitMQ.Client;

namespace MilestoneTG.ChangeStream.RabbitMQ;

public sealed class AmqpPublisher : IDestination
{
    readonly IConnection _connection;
    readonly string _topicName;
    
    static readonly string ConnectionStringName = "ConnectionStringName";
    static readonly string TopicName = "TopicName";
    
    static readonly JsonSerializerOptions JsonOptions;

    static AmqpPublisher()
    {
        JsonOptions = new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } };
    }
    
    public AmqpPublisher(Dictionary<string, object> settings, IConnectionStringFactory connectionStringFactory)
    {
        var connectionString = connectionStringFactory.GetConnectionString((string)settings[ConnectionStringName]);
        var factory = new ConnectionFactory
        {
            Uri = new Uri(connectionString)
        };
        _connection = factory.CreateConnection();
        _topicName = (string)settings[TopicName];
    }

    public async Task PublishAsync(ChangeEvent changeEvent)
    {
        using var buffer = new MemoryStream();
        await JsonSerializer.SerializeAsync(buffer, changeEvent, JsonOptions);
        using var channel = _connection.CreateModel();
        channel.BasicPublish(exchange: _topicName, routingKey: string.Empty, body: buffer.ToArray());
    }
}