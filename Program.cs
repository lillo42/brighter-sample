using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.DynamoDb;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.AWSSQS;
using Paramore.Brighter.Outbox.DynamoDB;
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
            var credentials = new BasicAWSCredentials("test", "test");
            var connection = new AWSMessagingGatewayConnection(credentials, RegionEndpoint.USEast1, cfg => cfg.ServiceURL = "http://localhost:4566");
            var dynamoDbClient = new AmazonDynamoDBClient(credentials, new AmazonDynamoDBConfig
            {
                RegionEndpoint = RegionEndpoint.USEast1,
                ServiceURL = "http://localhost:4566"
            });

            services
                .AddSingleton<IAmazonDynamoDB>(dynamoDbClient)
                .AddHostedService<ServiceActivatorHostedService>()
                .AddConsumers(opt =>
                {
                    opt.Subscriptions =
                    [
                        new SqsSubscription<OrderPlaced>(
                            new SubscriptionName("subscription"),
                            new ChannelName("queue-order-placed"),
                            ChannelType.PubSub,
                            new RoutingKey("order-placed"),
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Reactor
                        ),

                        new SqsSubscription<OrderPaid>(
                            new SubscriptionName("subscription"),
                            new ChannelName("queue-order-paid"),
                            ChannelType.PubSub,
                            new RoutingKey("order-paid"),
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Reactor
                        ),
                    ];

                    opt.DefaultChannelFactory = new ChannelFactory(connection);
                })
                .AutoFromAssemblies()
                .AddProducers(opt =>
                {
                    opt.Outbox = new DynamoDbOutbox(dynamoDbClient, 
                        new DynamoDbConfiguration
                        {
                            TimeToLive = TimeSpan.FromMinutes(1) 
                        });
                    opt.ConnectionProvider = typeof(DynamoDbUnitOfWork);
                    opt.TransactionProvider = typeof(DynamoDbUnitOfWork);
                    
                    opt.ProducerRegistry = new SnsProducerRegistryFactory(
                        connection,
                        [
                            new SnsPublication<OrderPaid>
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("order-paid"),
                            },
                            new SnsPublication<OrderPlaced>
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("order-placed"),
                            },
                        ]).Create();
                })
                .UseOutboxSweeper(opt => { opt.BatchSize = 10; })
                // .UseOutboxArchiver<DbTransaction>(new NullOutboxArchiveProvider())
                ;
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
    catch
    {
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

public class OrderPlaceHandler(ILogger<OrderPlaceHandler> logger) : RequestHandler<OrderPlaced>
{
    public override OrderPlaced Handle(OrderPlaced command)
    {
        logger.LogInformation("{OrderId} placed with value {OrderValue}", command.OrderId, command.Value);
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