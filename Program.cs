// See https://aka.ms/new-console-template for more information

using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.Inbox.DynamoDb.V4;
using Paramore.Brighter.Locking.DynamoDb.V4;
using Paramore.Brighter.MessagingGateway.Kafka;
using Paramore.Brighter.Outbox.DynamoDb.V4;
using Paramore.Brighter.Outbox.Hosting;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;

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
            var connection = new KafkaMessagingGatewayConfiguration
            {
                Name = "sample",
                BootStrapServers = ["localhost:9092"],
                SaslUsername = "admin",
                SaslPassword = "admin-secret",
                SecurityProtocol = SecurityProtocol.Plaintext,
                SaslMechanisms = SaslMechanism.Plain,
            };

            // DynamoDB Local for development using AWS SDK v4
            var dynamoDbConfig = new AmazonDynamoDBConfig
            {
                Region = "us-east-1",
                ServiceUrl = "http://localhost:8000"
            };

            var dynamoDbClient = new AmazonDynamoDBClient(dynamoDbConfig);

            services
                .AddSingleton<IAmazonDynamoDB>(dynamoDbClient)
                .AddHostedService<ServiceActivatorHostedService>()
                .AddConsumers(opt =>
                {
                    opt.Subscriptions =
                    [
                        new KafkaSubscription<OrderPlaced>(
                            new SubscriptionName("subscription"),
                            new ChannelName("order-placed"),
                            new RoutingKey("order-placed"),
                            groupId: "brighter-sample-placed",
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Proactor
                        ),

                        new KafkaSubscription<OrderPaid>(
                            new SubscriptionName("subscription"),
                            new ChannelName("order-paid"),
                            new RoutingKey("order-paid"),
                            groupId: "brighter-sample-order-paid",
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Proactor
                        )
                    ];

                    opt.DefaultChannelFactory = new ChannelFactory(new KafkaMessageConsumerFactory(connection));
                    opt.InboxConfiguration = new InboxConfiguration(new DynamoDbInboxV4(dynamoDbClient));
                })
                .AutoFromAssemblies()
                .AddProducers(opt =>
                {
                    opt.Outbox = new DynamoDbOutboxV4(dynamoDbClient);
                    opt.DistributedLock = new DynamoDbLockingProviderV4(dynamoDbClient);
                    opt.ConnectionProvider = typeof(DynamoDbConnectionProviderV4);
                    opt.TransactionProvider = typeof(DynamoDbUnitOfWorkV4);
                    
                    opt.ProducerRegistry = new KafkaProducerRegistryFactory(
                        connection,
                        [
                            new KafkaPublication<OrderPaid>
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("order-paid"),
                            },
                            new KafkaPublication<OrderPlaced>
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("order-placed"),
                            }
                        ]
                    ).Create();
                })
                .UseOutboxSweeper(opt => { opt.BatchSize = 10;  });

        }
    )
    .Build();


await host.StartAsync();

CancellationTokenSource cancellationTokenSource = new();

Console.CancelKeyPress += (_, _) => cancellationTokenSource.Cancel();

while (!cancellationTokenSource.IsCancellationRequested)
{
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
        await process.SendAsync(new CreateNewOrder { Value = value });
    }
    catch(Exception ex)
    {
        Console.WriteLine(ex.ToString());
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

public class CreateNewOrderHandler(IAmACommandProcessor commandProcessor, ILogger<CreateNewOrderHandler> logger) : RequestHandlerAsync<CreateNewOrder>
{
    public override async Task<CreateNewOrder> HandleAsync(CreateNewOrder command, CancellationToken cancellationToken = default)
    {
        try
        {
            var id = Uuid.NewAsString();
            logger.LogInformation("Creating a new order: {OrderId}", id);

            await commandProcessor.DepositPostAsync(new OrderPlaced { OrderId = id, Value = command.Value }, cancellationToken: cancellationToken);
            if (command.Value % 3 == 0)
            {
                throw new InvalidOperationException("invalid value");
            }

            await commandProcessor.DepositPostAsync(new OrderPaid { OrderId = id }, cancellationToken: cancellationToken);
            return await base.HandleAsync(command, cancellationToken);
        }
        catch(Exception ex)
        {
            logger.LogError(ex, "Invalid data");
            throw;
        }
    }
}

public class OrderPlaceHandler(ILogger<OrderPlaceHandler> logger) : RequestHandlerAsync<OrderPlaced>
{
    public override Task<OrderPlaced> HandleAsync(OrderPlaced command, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("{OrderId} placed with value {OrderValue}", command.OrderId, command.Value);
        return base.HandleAsync(command, cancellationToken);
    }
}

public class OrderPaidHandler(ILogger<OrderPaidHandler> logger) : RequestHandlerAsync<OrderPaid>
{
    public override Task<OrderPaid> HandleAsync(OrderPaid command, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("{OrderId} paid", command.OrderId);
        return base.HandleAsync(command, cancellationToken);
    }
}
