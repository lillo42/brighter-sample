using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.Inbox.Postgres;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;

const string connectionString = "Host=localhost;Username=postgres;Password=password;Database=brightertests;";
await using (NpgsqlConnection connection = new(connectionString))
{
    await connection.OpenAsync();
    await using var command = connection.CreateCommand();
    command.CommandText =
      """
      CREATE TABLE IF NOT EXISTS "inboxmessages"
      (
        CommandId VARCHAR(256) NOT NULL ,
        CommandType VARCHAR(256) NULL ,
        CommandBody TEXT NULL ,
        Timestamp timestamptz  NULL ,
        ContextKey VARCHAR(256) NULL,
        PRIMARY KEY (CommandId, ContextKey)
      ); 
      """;

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
        (_, services) =>
        {
            var connection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(new Uri("amqp://guest:guest@localhost:5672")),
                Exchange = new Exchange("paramore.brighter.exchange"),
            };

            services
                .AddHostedService<ServiceActivatorHostedService>()
                .AddConsumers(opt =>
                {
                    opt.InboxConfiguration = new InboxConfiguration(
                        new PostgreSqlInbox(new RelationalDatabaseConfiguration(connectionString, "brightertests", inboxTableName: "inboxmessages")));
                    
                    opt.Subscriptions =
                    [
                        new RmqSubscription<OrderPlaced>(
                            new SubscriptionName("subscription-orderplaced"),
                            new ChannelName("order-placed-queue"),
                            new RoutingKey("order-placed-topic"),
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Reactor
                        ),

                        new RmqSubscription<OrderPaid>(
                            new SubscriptionName("subscription-orderpaid"),
                            new ChannelName("order-paid-queue"),
                            new RoutingKey("order-paid-topic"),
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Reactor
                        ),
                    ];
                    
                    opt.DefaultChannelFactory = new ChannelFactory(new RmqMessageConsumerFactory(connection));
                })
                .AutoFromAssemblies()
                .AddProducers(opt =>
                {
                    opt.ProducerRegistry = new RmqProducerRegistryFactory(
                        connection,
                        [
                            new RmqPublication<OrderPaid>
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("order-paid-topic"),
                            },
                            new RmqPublication<OrderPlaced>
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("order-placed-topic"),

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
        await process.SendAsync(new CreateNewOrder { Value = value });
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
    ILogger<CreateNewOrderHandler> logger) : RequestHandlerAsync<CreateNewOrder>
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
        catch (Exception ex)
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
