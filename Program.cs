using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.Inbox;
using Paramore.Brighter.Inbox.Sqlite;
using Paramore.Brighter.MessagingGateway.Redis;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;

const string connectionString = "Data Source=brighter.db";

await using (SqliteConnection connection = new(connectionString))
{
    await connection.OpenAsync();

    await using var command = connection.CreateCommand();
    command.CommandText =
      """
      CREATE TABLE IF NOT EXISTS "inbox_messages"(
        [CommandId] uniqueidentifier CONSTRAINT PK_MessageId PRIMARY KEY,
        [CommandType] nvarchar(256),
        [CommandBody] ntext,
        [Timestamp] TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        [ContextKey] nvarchar(256)
      );
      """;
    // MessageId, MessageType, Topic, Timestamp, CorrelationId, ReplyTo, ContentType, HeaderBag, Body
    _ = await command.ExecuteNonQueryAsync();

}

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Paramore.Brighter", Serilog.Events.LogEventLevel.Debug)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();

var host = new HostBuilder()
    .UseSerilog()
    .ConfigureServices(
        (ctx, services) =>
        {
            var connection = new RedisMessagingGatewayConfiguration
            {
                RedisConnectionString = "localhost:6379?connectTimeout=1000&sendTimeout=1000&",
                MaxPoolSize = 10,
                MessageTimeToLive = TimeSpan.FromMinutes(10)
            };
            
            var configuration = new RelationalDatabaseConfiguration(connectionString, "brighter", inboxTableName: "inbox_messages");

            services
                .AddSingleton<IAmARelationalDatabaseConfiguration >(configuration)
                .AddHostedService<ServiceActivatorHostedService>()
                .AddConsumers(opt =>
                {
                    opt.InboxConfiguration = new InboxConfiguration(new SqliteInbox(configuration), actionOnExists: OnceOnlyAction.Warn);
                    opt.Subscriptions =
                    [
                        new RedisSubscription<OrderPlaced>(
                            new SubscriptionName("subscription"),
                            new ChannelName("queue-order-placed"),
                            new RoutingKey("order-placed"),
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Proactor 
                        ),

                        new RedisSubscription<OrderPaid>(
                            new SubscriptionName("subscription"),
                            new ChannelName("queue-order-paid"),
                            new RoutingKey("order-paid"),
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Proactor
                        ),
                    ];

                    opt.DefaultChannelFactory = new ChannelFactory(
                        new RedisMessageConsumerFactory(connection)
                    );
                })
                .AutoFromAssemblies()
                .AddProducers(opt =>
                {
                    opt.ProducerRegistry = new RedisProducerRegistryFactory(
                        connection,
                        [
                            new RedisMessagePublication<OrderPaid>
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("order-paid"),
                            },
                            new RedisMessagePublication<OrderPlaced>
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("order-placed"),
                            },
                        ]).Create();
                });
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

public class CreateNewOrderHandler(IAmACommandProcessor commandProcessor,
    ILogger<CreateNewOrderHandler> logger) : RequestHandlerAsync<CreateNewOrder>
{
    public override async Task<CreateNewOrder> HandleAsync(CreateNewOrder command, CancellationToken cancellationToken = default)
    {
        try
        {
            var id = Uuid.NewAsString();
            logger.LogInformation("Creating a new order: {OrderId}", id);

            await commandProcessor.PostAsync(new OrderPlaced { OrderId = id, Value = command.Value }, cancellationToken: cancellationToken);
            await commandProcessor.PostAsync(new OrderPaid { OrderId = id }, cancellationToken: cancellationToken);

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