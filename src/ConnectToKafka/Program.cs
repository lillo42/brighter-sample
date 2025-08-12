using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessageMappers;
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
        (_, services) =>
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
                .AddConsumers(opt =>
                {
                    opt.Subscriptions =
                    [
                        new KafkaSubscription<Greeting>(
                            new SubscriptionName("kafka.greeting.subscription"),
                            new ChannelName("greeting.topic"),
                            new RoutingKey("greeting.topic"),
                            groupId: "some-consumer-group",
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Reactor
                        )
                    ];

                    opt.DefaultChannelFactory = new ChannelFactory(new KafkaMessageConsumerFactory(connection));
                })
                .AutoFromAssemblies()
                .MapperRegistry(registry => registry.SetDefaultMessageMapper(typeof(CloudEventJsonMessageMapper<>)))
                .AddProducers(opt =>
                {
                    opt.ProducerRegistry = new KafkaProducerRegistryFactory(
                        connection,
                        [
                            new KafkaPublication
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Source = new Uri("test-app", UriKind.RelativeOrAbsolute),
                                Topic = new RoutingKey("greeting.topic")
                            }
                        ]
                    ).Create();
                });
        }
    )
    .Build();

_ = host.RunAsync();

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

[PublicationTopic("greeting.topic")]
public class Greeting() : Event(Id.Random())
{
    public string Name { get; set; } = string.Empty;
}

public class GreetingHandler(ILogger<GreetingHandler> logger) : RequestHandler<Greeting>
{
    public override Greeting Handle(Greeting command)
    {
        logger.LogInformation("Hello {Name}", command.Name);
        return base.Handle(command);
    }
}
