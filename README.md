# Change Stream

The goal of this project is to stream SQL Server CDC events to a RabbitMQ topic.

Should be pluggable for other databases and message brokers.

![diagram](change_stream.png)

## Configuration

Configuration Section: Cdc

The Streams property is an array defining the source, destination, and resiliency parameters.

### Example:

``` json
  "Cdc": {
    "Streams": [
    {
      "CheckIntervalInMilliseconds": 1000,
      "CircuitBreakerTimeout": "00:00:05",
      "Source" : {
        "SourceName": "SqlDb",
        "SourceType": "Tortis.ChangeStream.SqlServer.SqlServerSource,Tortis.ChangeStream.SqlServer",
        "Properties": {
          "ConnectionStringName": "SqlDb",
          "SchemaName": "dbo",
          "TableName": "person"
        }
      },
      "Destination" : {
        "DestinationName": "EventBus",
        "DestinationType": "Tortis.ChangeStream.RabbitMQ.AmqpPublisher,Tortis.ChangeStream.RabbitMQ",
        "Properties": {
          "ConnectionStringName": "RabbitMQ",
          "TopicName": "dbo_person"
        }
      }
    }
  ]}
```

## License
[Licensed under MIT](LICENSE)
