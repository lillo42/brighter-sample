using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();

var host = new HostBuilder()
    .UseSerilog()
    .ConfigureServices(
        (ctx, services) =>
        {
            var connection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(new Uri("amqp://guest:guest@localhost:5672")),
                Exchange = new Exchange("paramore.brighter.exchange"),
            };

            services
                .AddHostedService<ServiceActivatorHostedService>()
                .AddServiceActivator(opt =>
                {
                    opt.Subscriptions =
                    [
                        new RmqSubscription<Greeting>(
                            new SubscriptionName("kafka.greeting.subscription"),
                            new ChannelName("greeting.topic"),
                            new RoutingKey("greeting.topic"),
                            makeChannels: OnMissingChannel.Create
                        ),
                    ];

                    opt.DefaultChannelFactory = new ChannelFactory(new RmqMessageConsumerFactory(connection));
                })
                .AutoFromAssemblies()
                .UseExternalBus(opt =>
                    {
                        opt.ProducerRegistry = new RmqProducerRegistryFactory(
                            connection, [
                                new RmqPublication<Greeting>
                                {
                                    MakeChannels = OnMissingChannel.Create,
                                    Topic = new RoutingKey("greeting.topic"),
                                },
                            ]
                        ).Create();
                    }
                );
        }
    )
    .Build();

await host.StartAsync();

while (true)
{
    await Task.Delay(TimeSpan.FromSeconds(10));
    Console.Write("Say your name (or q to quit): ");
    var name = Console.ReadLine();

    if (string.IsNullOrEmpty(name))
    {
        continue;
    }

    if (name == "q")
    {
        break;
    }

    var process = host.Services.GetRequiredService<IAmACommandProcessor>();
    process.Post(new Greeting { Name = name });
}

await host.StopAsync();

public class Greeting() : Event(Guid.NewGuid())
{
    public string Name { get; set; } = string.Empty;
}

public class GreetingMapper : IAmAMessageMapper<Greeting>
{
    public Message MapToMessage(Greeting request, Publication publication)
    {
        var header = new MessageHeader
        {
            MessageId = request.Id,
            TimeStamp = DateTimeOffset.UtcNow,
            Topic = publication.Topic!,
            MessageType = MessageType.MT_EVENT
        };

        var body = new MessageBody(JsonSerializer.Serialize(request));
        return new Message(header, body);
    }

    public Greeting MapToRequest(Message message)
    {
        return JsonSerializer.Deserialize<Greeting>(message.Body.Bytes)!;
    }

    public IRequestContext? Context { get; set; }
}

public class GreetingHandler(ILogger<GreetingHandler> logger) : RequestHandler<Greeting>
{
    public override Greeting Handle(Greeting command)
    {
        logger.LogInformation("Hello {Name}", command.Name);
        return base.Handle(command);
    }
}