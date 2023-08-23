using RabbitMQ.Client;

namespace MilestoneTG.ChangeStream.RabbitMQ;

public class AmqpPublisher : IDestination
{
    readonly IConnection _connection;

    public AmqpPublisher()
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.Uri = new Uri("amqp://user:Password1@localhost:5672/");
        _connection = factory.CreateConnection();
    }

    public Task PublishAsync(ChangeEvent changeEvent)
    {
        using var channel = _connection.CreateModel();

        using var buffer = new MemoryStream();
        ProtoBuf.Serializer.Serialize(buffer, changeEvent);
        channel.BasicPublish(exchange: "", routingKey: string.Empty, body: buffer.ToArray());
        
        return Task.CompletedTask;
    }
}