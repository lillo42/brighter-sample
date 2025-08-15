using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.Kafka;
using Paramore.Brighter.MySql;
using Paramore.Brighter.Outbox.Hosting;
using Paramore.Brighter.Outbox.MySql;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;

const string connectionString = "server=127.0.0.1;uid=root;pwd=Password123!;database=brighter_test";

await using (MySqlConnection connection = new(connectionString))
{
    await connection.OpenAsync();

    await using var command = connection.CreateCommand();
    command.CommandText =
      """
      CREATE TABLE IF NOT EXISTS `outbox_messages`(
        `MessageId`VARCHAR(255) NOT NULL , 
        `Topic` VARCHAR(255) NOT NULL , 
        `MessageType` VARCHAR(32) NOT NULL , 
        `Timestamp` TIMESTAMP(3) NOT NULL , 
        `CorrelationId`VARCHAR(255) NULL ,
        `ReplyTo` VARCHAR(255) NULL ,
        `ContentType` VARCHAR(128) NULL , 
        `PartitionKey` VARCHAR(128) NULL , 
        `WorkflowId` VARCHAR(255) NULL ,
        `JobId` VARCHAR(255) NULL ,
        `Dispatched` TIMESTAMP(3) NULL , 
        `HeaderBag` TEXT NOT NULL , 
        `Body` TEXT NOT NULL , 
        `Source`  VARCHAR(255) NULL,
        `Type`  VARCHAR(255) NULL,
        `DataSchema`  VARCHAR(255) NULL,
        `Subject`  VARCHAR(255) NULL,
        `TraceParent`  VARCHAR(255) NULL,
        `TraceState`  VARCHAR(255) NULL,
        `Baggage`  TEXT NULL,
        `Created` TIMESTAMP(3) NOT NULL DEFAULT NOW(3),
        `CreatedID` INT(11) NOT NULL AUTO_INCREMENT, 
        UNIQUE(`CreatedID`),
        PRIMARY KEY (`MessageId`)
      );
      """;
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
        (ctx, services) =>
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


            var configuration = new RelationalDatabaseConfiguration(connectionString,  "brighter_test", "outbox_messages");

            services
                .AddSingleton<IAmARelationalDatabaseConfiguration>(configuration)
                .AddHostedService<ServiceActivatorHostedService>()
                .AddConsumers(opt =>
                {
                    opt.Subscriptions =
                    [
                        new KafkaSubscription<OrderPlaced>(
                            new SubscriptionName("subscription"),
                            new ChannelName("order-placed"),
                            new RoutingKey("order-placed"),
                            groupId: "brighter-sample",
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Proactor
                        ),

                        new KafkaSubscription<OrderPaid>(
                            new SubscriptionName("subscription"),
                            new ChannelName("order-paid"),
                            new RoutingKey("order-paid"),
                            groupId: "brighter-sample",
                            makeChannels: OnMissingChannel.Create,
                            messagePumpType: MessagePumpType.Proactor
                        )
                    ];

                    opt.DefaultChannelFactory = new ChannelFactory(
                        new KafkaMessageConsumerFactory(connection)
                    );
                })
                .AutoFromAssemblies()
                .AddProducers(opt =>
                {
                    opt.Outbox = new MySqlOutbox(configuration);
                    opt.ConnectionProvider = typeof(MySqlConnectionProvider);
                    opt.TransactionProvider = typeof(MySqlUnitOfWork);
                    
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
                .UseOutboxSweeper(opt => { opt.BatchSize = 10; });

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