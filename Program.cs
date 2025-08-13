using Hangfire;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessageScheduler.Hangfire;
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
            .AddHangfire(opt => opt
                .UseDefaultActivator()
                .UseRecommendedSerializerSettings()
                .UseSimpleAssemblyNameTypeSerializer()
                .UseInMemoryStorage())
            .AddHangfireServer();
            
        services
            .AddHostedService<ServiceActivatorHostedService>()
            .AddConsumers(opt =>
            {
                opt.Subscriptions = [
                    new PostgresSubscription<SchedulerCommand>(
                        subscriptionName: new SubscriptionName("greeting.subscription"),
                        channelName: new ChannelName("greeting.topic"),
                        makeChannels: OnMissingChannel.Create,
                        messagePumpType: MessagePumpType.Proactor,
                        timeOut:  TimeSpan.FromSeconds(10)
                    )
                ];

                opt.DefaultChannelFactory= new PostgresChannelFactory(new PostgresMessagingGatewayConnection(config));
            })
            .UseScheduler(_ => new HangfireMessageSchedulerFactory())
            .AddProducers(opt =>
            {
                opt.ProducerRegistry = new  PostgresProducerRegistryFactory(new PostgresMessagingGatewayConnection(config), [
                    new PostgresPublication<SchedulerCommand>
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
    await process.PostAsync(TimeSpan.FromSeconds(1), new SchedulerCommand { Name = name, Type = "Post"});
    await process.SendAsync(TimeSpan.FromSeconds(2), new SchedulerCommand { Name = name, Type = "Send"});
    await process.PublishAsync(DateTimeOffset.UtcNow + TimeSpan.FromSeconds(3), new SchedulerCommand { Name = name, Type = "Publish"});
}

await host.StopAsync();


public class SchedulerCommand() : Event(Guid.NewGuid())
{
    public string Name { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
}

public class SchedulerCommandHandler(ILogger<SchedulerCommandHandler> logger) : RequestHandlerAsync<SchedulerCommand>
{
    public override Task<SchedulerCommand> HandleAsync(SchedulerCommand command, CancellationToken cancellationToken = new CancellationToken())
    {
        logger.LogInformation("Hello {Name} (with Type: {Type})", command.Name, command.Type);
        return base.HandleAsync(command, cancellationToken);
    }
}