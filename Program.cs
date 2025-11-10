using Fluent.Brighter;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Paramore.Brighter;
using Paramore.Brighter.Inbox.Postgres;
using Paramore.Brighter.MessagingGateway.Kafka;
using Paramore.Brighter.Policies.Attributes;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Polly;
using Polly.Retry;
using Serilog;

const string kafkaPolicy = "kafka-policy";
const string connectionString = "Host=localhost;Username=postgres;Password=password;Database=brightertests;";
await using (NpgsqlConnection connection = new(connectionString))
{
    await connection.OpenAsync();
    await using var command = connection.CreateCommand();
    
    command.CommandText = PostgreSqlInboxBuilder.GetDDL("inboxmessages");
    _ = await command.ExecuteNonQueryAsync();
}

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Paramore.Brighter", Serilog.Events.LogEventLevel.Warning)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();

var host = new HostBuilder()
    .UseSerilog()
    .ConfigureServices(
        (_, services) =>
        {
            services
                .AddHostedService<ServiceActivatorHostedService>()
                .AddFluentBrighter(opt => opt
                    .Subscriptions(c => c.AddResiliencePipeline<object>(kafkaPolicy, (builder, _) => builder
                        .AddRetry(new RetryStrategyOptions
                        {
                            Delay = TimeSpan.FromSeconds(1),
                            MaxRetryAttempts = 3
                        })))
                    .UsingPostgres(pg => pg
                        .SetConnection(db => db
                            .SetConnectionString(connectionString)
                            .SetDatabaseName("brightertests")
                            .SetInboxTableName("inboxmessages"))
                        .UseInbox())
                    .UsingKafka(kf => kf
                        .SetConnection(c => c
                            .SetName("sample")
                            .SetBootstrapServers("localhost:9092")
                            .SetSecurityProtocol(SecurityProtocol.Plaintext)
                            .SetSaslMechanisms(SaslMechanism.Plain))
                        .UseSubscriptions(s => s
                            .AddSubscription<OrderPlaced>(sb => sb
                                .SetTopic("order-placed-topic")
                                .SetConsumerGroupId("order-placed-topic-1")
                                .SetRequeueCount(3)
                                .CreateInfrastructureIfMissing()
                                .UseReactorMode())
                            .AddSubscription<OrderPaid>(sb => sb
                                .SetTopic("order-paid-topic")
                                .SetConsumerGroupId("order-paid-topic-1")
                                .CreateInfrastructureIfMissing()
                                .UseReactorMode()))
                        .UsePublications(p => p
                            .AddPublication<OrderPaid>(kp => kp
                                .SetTopic("order-paid-topic")
                                .CreateTopicIfMissing())
                            .AddPublication<OrderPlaced>(kp => kp
                                .SetTopic("order-placed-topic")
                                .CreateTopicIfMissing()))));

            // var connection = new KafkaMessagingGatewayConfiguration
            // {
            //     Name = "sample",
            //     BootStrapServers = ["localhost:9092"],
            //     SaslUsername = "admin",
            //     SaslPassword = "admin-secret",
            //     SecurityProtocol = SecurityProtocol.Plaintext,
            //     SaslMechanisms = SaslMechanism.Plain,
            // };
            //
            // services
            //     .AddHostedService<ServiceActivatorHostedService>()
            //     .AddConsumers(opt =>
            //     {
            //         opt.InboxConfiguration = new InboxConfiguration(new PostgreSqlInbox(new RelationalDatabaseConfiguration(connectionString, "brightertests", inboxTableName: "inboxmessages")));
            //         
            //         opt.Subscriptions =
            //         [
            //             new KafkaSubscription<OrderPlaced>(
            //                 new SubscriptionName("subscription-orderplaced"),
            //                 new ChannelName("order-placed-queue"),
            //                 new RoutingKey("order-placed-topic"),
            //                 makeChannels: OnMissingChannel.Create,
            //                 messagePumpType: MessagePumpType.Reactor,
            //                 groupId: "test"
            //             ),
            //
            //             new KafkaSubscription<OrderPaid>(
            //                 new SubscriptionName("subscription-orderpaid"),
            //                 new ChannelName("order-paid-queue"),
            //                 new RoutingKey("order-paid-topic"),
            //                 makeChannels: OnMissingChannel.Create,
            //                 messagePumpType: MessagePumpType.Reactor,
            //                 groupId: "test"
            //             ),
            //         ];
            //         
            //         opt.DefaultChannelFactory = new ChannelFactory(new KafkaMessageConsumerFactory(connection));
            //     })
            //     .AutoFromAssemblies()
            //     .AddProducers(opt =>
            //     {
            //         opt.ProducerRegistry = new KafkaProducerRegistryFactory(
            //             connection,
            //             [
            //                 new KafkaPublication<OrderPaid>
            //                 {
            //                     MakeChannels = OnMissingChannel.Create,
            //                     Topic = new RoutingKey("order-paid-topic"),
            //                 },
            //                 new KafkaPublication<OrderPlaced>
            //                 {
            //                     MakeChannels = OnMissingChannel.Create,
            //                     Topic = new RoutingKey("order-placed-topic"),
            //                 }
            //             ]).Create();
            //     });
        }
    )
    .Build();

await host.StartAsync();

CancellationTokenSource cancellationTokenSource = new();
Console.CancelKeyPress += (_, _) => cancellationTokenSource.Cancel();

while (!cancellationTokenSource.IsCancellationRequested)
{
    await Task.Delay(TimeSpan.FromSeconds(10));
    Console.Write("Type an order value (or q to quit): ");
    var tmp = Console.ReadLine();

    if (string.IsNullOrEmpty(tmp))
    {
        continue;
    }

    if (tmp == "q")
    {
        break;
    }

    if (!decimal.TryParse(tmp, out var value))
    {
        continue;
    }

    try
    {
        using var scope = host.Services.CreateScope();
        var process = scope.ServiceProvider.GetRequiredService<IAmACommandProcessor>();
        process.Send(new CreateNewOrder { Value = value });
    }
    catch(Exception ex)
    {
        Console.WriteLine($"Error: {ex}");
        // ignore any error
    }
}

await host.StopAsync();

public class CreateNewOrder() : Command(Id.Random())
{
    public decimal Value { get; set; }
}

public class OrderPlaced() : Event(Id.Random())
{
    public string OrderId { get; set; } = string.Empty;

    public decimal Value { get; set; }
}

public class OrderPaid() : Event(Id.Random())
{
    public string OrderId { get; set; } = string.Empty;
}

public class CreateNewOrderHandler(IAmACommandProcessor commandProcessor,
    ILogger<CreateNewOrderHandler> logger) : RequestHandler<CreateNewOrder>
{
    public override CreateNewOrder Handle(CreateNewOrder command)
    {
        try
        {
            var id = Uuid.NewAsString();
            logger.LogInformation("Creating a new order: {OrderId}", id);

            commandProcessor.Post(new OrderPlaced { OrderId = id, Value = command.Value });
            commandProcessor.Post(new OrderPaid { OrderId = id });
            return base.Handle(command);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Invalid data");
            throw;
        }
    }
}

public class OrderPlaceHandler(ILogger<OrderPlaceHandler> logger) : RequestHandler<OrderPlaced>
{
    [UseResiliencePipeline("kafka-policy", 1)]
    public override OrderPlaced Handle(OrderPlaced command)
    {
        logger.LogInformation("{OrderId} placed with value {OrderValue}", command.OrderId, command.Value);
        if (command.Value % 3 == 0)
        {
            logger.LogError("Simulate an error for {OrderId} with value {OrderValue}", command.OrderId, command.Value);
            throw new InvalidOperationException("invalid error");
        }
        
        return base.Handle(command);
    }
}


public class OrderPaidHandler(ILogger<OrderPaidHandler> logger) : RequestHandler<OrderPaid>
{
    public override OrderPaid Handle(OrderPaid command)
    {
        logger.LogInformation("{OrderId} paid", command.OrderId);
        return base.Handle(command);
    }
}
