using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using Paramore.Brighter.MsSql;
using Paramore.Brighter.Outbox.Hosting;
using Paramore.Brighter.Outbox.MsSql;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;

const string connectionString = "Server=127.0.0.1,1433;Database=BrighterTests;User Id=sa;Password=Password123!;Application Name=BrighterTests;Connect Timeout=60;Encrypt=false;";

await using (SqlConnection connection = new("Server=127.0.0.1,1433;Database=master;User Id=sa;Password=Password123!;Application Name=BrighterTests;Connect Timeout=60;Encrypt=false;"))
{
    await connection.OpenAsync();

    await using var command = connection.CreateCommand();
    command.CommandText =
      """
      IF DB_ID('BrighterTests') IS NULL
      BEGIN
        CREATE DATABASE BrighterTests;
      END;
      """;
    _ = await command.ExecuteNonQueryAsync();

}

await using (SqlConnection connection = new(connectionString))
{
    await connection.OpenAsync();
    await using var command = connection.CreateCommand();
    command.CommandText =
      """
      IF OBJECT_ID('OutboxMessages', 'U') IS NULL
      BEGIN 
        CREATE TABLE [OutboxMessages]
        (
          [Id] [BIGINT] NOT NULL IDENTITY,
          [MessageId] NVARCHAR(255) NOT NULL,
          [Topic] NVARCHAR(255) NULL,
          [MessageType] NVARCHAR(32) NULL,
          [Timestamp] DATETIME NULL,
          [CorrelationId] NVARCHAR(255) NULL,
          [ReplyTo] NVARCHAR(255) NULL,
          [ContentType] NVARCHAR(128) NULL,  
          [PartitionKey] NVARCHAR(255) NULL,
          [WorkflowId] NVARCHAR(255) NULL,
          [JobId] NVARCHAR(255) NULL,
          [Dispatched] DATETIME NULL,
          [HeaderBag] NVARCHAR(MAX) NULL,
          [Body] VARBINARY(MAX) NULL,
          [Source] NVARCHAR(255) NULL,
          [Type] NVARCHAR(255) NULL,
          [DataSchema] NVARCHAR(255) NULL,
          [Subject] NVARCHAR(255) NULL,
          [TraceParent] NVARCHAR(255) NULL,
          [TraceState] NVARCHAR(255) NULL,
          [Baggage] NVARCHAR(MAX) NULL,
          PRIMARY KEY ( [Id] ) 
        );
      END 
      """;
    try
    {
        _ = await command.ExecuteNonQueryAsync();
    }
    catch
    {
        // Ignore if it exists
    }
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
        (ctx, services) =>
        {
            var connection = new RmqMessagingGatewayConnection ()
            {
                AmpqUri = new AmqpUriSpecification(new Uri("amqp://guest:guest@localhost:5672")),
                Exchange = new Exchange("paramore.brighter.exchange")
            };

            var configuration = new RelationalDatabaseConfiguration(connectionString, "BrighterTests", "OutboxMessages", binaryMessagePayload: true);

            services
                .AddSingleton<IAmARelationalDatabaseConfiguration >(configuration)
                .AddHostedService<ServiceActivatorHostedService>()
                .AddConsumers(opt =>
                {
                    opt.Subscriptions =
                    [
                        new RmqSubscription<OrderPlaced>(
                            new SubscriptionName("subscription"),
                            new ChannelName("queue-order-placed"),
                            new RoutingKey("order-placed"),
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Reactor
                        ),

                        new RmqSubscription<OrderPaid>(
                            new SubscriptionName("subscription"),
                            new ChannelName("queue-order-paid"),
                            new RoutingKey("order-paid"),
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Reactor
                        ),
                    ];

                    opt.DefaultChannelFactory = new ChannelFactory(
                        new RmqMessageConsumerFactory(connection)
                    );
                })
                .AutoFromAssemblies()
                .AddProducers(opt =>
                {
                    opt.Outbox = new MsSqlOutbox(configuration);
                    opt.ConnectionProvider = typeof(MsSqlConnectionProvider);
                    opt.TransactionProvider = typeof(MsSqlUnitOfWork);
                    
                    opt.ProducerRegistry = new RmqProducerRegistryFactory(
                        connection,
                        [
                            new RmqPublication<OrderPaid>
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("order-paid"),
                            },
                            new RmqPublication<OrderPlaced>
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("order-placed"),
                            },
                        ]).Create();
                })
                .UseOutboxSweeper(opt => { opt.BatchSize = 10; })
                .UseOutboxArchiver<DbTransaction>(new NullOutboxArchiveProvider());
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