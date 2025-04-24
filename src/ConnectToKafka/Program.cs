// See https://aka.ms/new-console-template for more information
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.Kafka;
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
            var connection = new KafkaMessagingGatewayConfiguration
            {
                Name = "sample",
                BootStrapServers = ["localhost:9092"],
                SaslUsername = "admin",
                SaslPassword = "admin-secret",
                SecurityProtocol = SecurityProtocol.Plaintext,
                SaslMechanisms = SaslMechanism.Plain,
            };

            services
                .AddHostedService<ServiceActivatorHostedService>()
                .AddServiceActivator(opt =>
                {
                    opt.Subscriptions =
                    [
                        new KafkaSubscription<Greeting>(
                            new SubscriptionName("kafka.greeting.subscription"),
                            new ChannelName("greeting.topic"),
                            new RoutingKey("greeting.topic"),
                            groupId: "some-consumer-group",
                            makeChannels: OnMissingChannel.Create,
                            numOfPartitions: 2,
                            isAsync: true
                        ),
                    ];

                    opt.ChannelFactory = new ChannelFactory(
                        new KafkaMessageConsumerFactory(connection)
                    );
                })
                .AutoFromAssemblies()
                .UseExternalBus(
                    new KafkaProducerRegistryFactory(
                        connection,
                        [
                            new KafkaPublication
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("greeting.topic"),
                            },
                        ]
                    ).Create()
                );
        }
    )
    .Build();

_ = host.RunAsync();

while (true)
{
    await Task.Delay(TimeSpan.FromSeconds(2));
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

host.WaitForShutdown();

public class Greeting() : Event(Guid.NewGuid())
{
    public string Name { get; set; } = string.Empty;
}

public class GreetingMapper : IAmAMessageMapper<Greeting>
{
    public Message MapToMessage(Greeting request)
    {
        var header = new MessageHeader();
        header.Id = request.Id;
        header.TimeStamp = DateTime.UtcNow;
        header.Topic = "greeting.topic";
        header.MessageType = MessageType.MT_EVENT;

        var body = new MessageBody(JsonSerializer.Serialize(request));
        return new Message(header, body);
    }

    public Greeting MapToRequest(Message message)
    {
        return JsonSerializer.Deserialize<Greeting>(message.Body.Bytes)!;
    }
}

public class GreetingHandler(ILogger<GreetingHandler> logger) : RequestHandlerAsync<Greeting>
{
    public override async Task<Greeting> HandleAsync(Greeting command, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Processing {Name}", command.Name);
        await Task.Delay(1_000);  // Simulate async I/O (e.g., HTTP call)
        logger.LogInformation("Hello {Name}", command.Name);
        return await base.HandleAsync(command, cancellationToken);
    }
}
