// See https://aka.ms/new-console-template for more information
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.RMQ;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();

var host = new HostBuilder()
    .UseSerilog()
    .ConfigureServices(
        (context, services) =>
        {
            var rmqConnection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(new Uri("amqp://guest:guest@localhost:5672")),
                Exchange = new Exchange("paramore.brighter.exchange"),
            };

            services
                .AddHostedService<ServiceActivatorHostedService>()
                .AddServiceActivator(opt =>
                {
                    opt.Subscriptions = new Subscription[]
                    {
                        new RmqSubscription<Greeting>(
                            new SubscriptionName("paramore.example.greeting"),
                            new ChannelName("greeting.event"),
                            new RoutingKey("greeting.event"),
                            makeChannels: OnMissingChannel.Create
                        ),
                    };

                    opt.ChannelFactory = new ChannelFactory(
                        new RmqMessageConsumerFactory(rmqConnection)
                    );
                })
                .UseExternalBus(
                    new RmqProducerRegistryFactory(
                        rmqConnection,
                        new RmqPublication[]
                        {
                            new()
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("greeting.event"),
                            },
                        }
                    ).Create()
                )
                .Handlers(register => register.Register<Greeting, GreetingHandler>())
                .MapperRegistry(register => register.Register<Greeting, GreetingMapper>());
        }
    )
    .Build();

_ = host.RunAsync();

while (true)
{
    Console.Write("Say your name (q to quit): ");
    var name = Console.ReadLine();
    if (string.IsNullOrEmpty(name))
    {
        continue;
    }

    if (name == "q")
    {
        break;
    }

    var processor = host.Services.GetRequiredService<IAmACommandProcessor>();
    processor.Post(new Greeting(Guid.NewGuid()) { Name = name });
}

host.WaitForShutdown();

public class Greeting(Guid id) : Event(id)
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
        header.Topic = "greeting.event";
        header.MessageType = MessageType.MT_EVENT;

        var body = new MessageBody(
            JsonSerializer.Serialize(request, JsonSerialisationOptions.Options)
        );

        return new Message(header, body);
    }

    public Greeting MapToRequest(Message message)
    {
        return JsonSerializer.Deserialize<Greeting>(message.Body.Bytes)!;
    }
}

public class GreetingHandler : RequestHandler<Greeting>
{
    private readonly ILogger<GreetingHandler> _logger;

    public GreetingHandler(ILogger<GreetingHandler> logger)
    {
        _logger = logger;
    }

    public override Greeting Handle(Greeting command)
    {
        _logger.LogInformation("Hello {Name}", command.Name);
        return base.Handle(command);
    }
}
