using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.Postgres;
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
    .ConfigureServices(static (_, services) =>
    {
        var config = new RelationalDatabaseConfiguration ("Host=localhost;Username=postgres;Password=password;Database=brightertests;", queueStoreTable: "QueueData");
        services
            .AddHostedService<ServiceActivatorHostedService>()
            .AddServiceActivator(opt =>
            {
                opt.Subscriptions = [
                    new PostgresSubscription<Greeting>(
                        subscriptionName: new SubscriptionName("greeting.subscription"),
                        channelName: new ChannelName("greeting.topic"),
                        makeChannels: OnMissingChannel.Create,
                        messagePumpType: MessagePumpType.Reactor,
                        timeOut:  TimeSpan.FromSeconds(10)
                    )
                ];

                opt.DefaultChannelFactory= new PostgresChannelFactory(new PostgresMessagingGatewayConnection(config));
            })
            .UseExternalBus(opt =>
            {
                opt.ProducerRegistry = new  PostgresProducerRegistryFactory(new PostgresMessagingGatewayConnection(config), [
                    new PostgresPublication<Greeting>
                    {
                        Topic = new RoutingKey("greeting.topic"),
                        MakeChannels = OnMissingChannel.Create
                    }
                ]).Create();
            })
            .AutoFromAssemblies();
    })
    .Build();

await host.StartAsync();

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

await host.StopAsync();


public class Greeting() : Event(Guid.NewGuid())
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