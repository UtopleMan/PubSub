namespace PubSub.RabbitMQ;

public class RabbitMqOptions
{
    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";
    public string Exchange { get; set; } = "events";
}
