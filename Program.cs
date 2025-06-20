using System.Data;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.Extensions.Hosting;
using Paramore.Brighter.MessagingGateway.RMQ;
using Paramore.Brighter.Outbox.Sqlite;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Paramore.Brighter.Sqlite;
using Serilog;

const string ConnectionString = "Data Source=brighter.db";

await using (SqliteConnection connection = new(ConnectionString))
{
    await connection.OpenAsync();

    await using var command = connection.CreateCommand();
    command.CommandText =
      """
      CREATE TABLE IF NOT EXISTS "outbox_messages"(
        [MessageId] TEXT NOT NULL COLLATE NOCASE,
        [Topic] TEXT NULL,
        [MessageType] TEXT NULL,
        [Timestamp] TEXT NULL,
        [CorrelationId] TEXT NULL,
        [ReplyTo] TEXT NULL,
        [ContentType] TEXT NULL,  
        [Dispatched] TEXT NULL,
        [HeaderBag] TEXT NULL,
        [Body] TEXT NULL,
        CONSTRAINT[PK_MessageId] PRIMARY KEY([MessageId])
      );
      """;
    // MessageId, MessageType, Topic, Timestamp, CorrelationId, ReplyTo, ContentType, HeaderBag, Body
    _ = await command.ExecuteNonQueryAsync();

}

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Paramore.Brighter", Serilog.Events.LogEventLevel.Warning)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();

IHost host = new HostBuilder()
    .UseSerilog()
    .ConfigureServices(
        (ctx, services) =>
        {
            RmqMessagingGatewayConnection connection = new()
            {
                AmpqUri = new AmqpUriSpecification(new Uri("amqp://guest:guest@localhost:5672")),

                Exchange = new Exchange("paramore.brighter.exchange"),
            };

            services
              .AddScoped<SqliteUnitOfWork, SqliteUnitOfWork>()
              .TryAddScoped<IUnitOfWork>(provider => provider.GetRequiredService<SqliteUnitOfWork>());

            _ = services
                .AddHostedService<ServiceActivatorHostedService>()
                .AddServiceActivator(opt =>
                {
                    opt.Subscriptions =
                    [
                        new RmqSubscription<OrderPlaced>(
                            new SubscriptionName("subscription"),
                            new ChannelName("queue-order-placed"),
                            new RoutingKey("order-placed"),
                            makeChannels: OnMissingChannel.Create,
                            runAsync: true
                        ),

                        new RmqSubscription<OrderPaid>(
                            new SubscriptionName("subscription"),
                            new ChannelName("queue-order-paid"),
                            new RoutingKey("order-paid"),
                            makeChannels: OnMissingChannel.Create,
                            runAsync: true
                        ),
                    ];

                    opt.ChannelFactory = new ChannelFactory(
                        new RmqMessageConsumerFactory(connection)
                    );
                })
                .AutoFromAssemblies()
                .UseSqliteOutbox(new SqliteConfiguration(ConnectionString, "outbox_messages"), typeof(SqliteConnectionProvider), ServiceLifetime.Scoped)
                .UseSqliteTransactionConnectionProvider(typeof(SqliteConnectionProvider))
                .UseOutboxSweeper(opt =>
                {
                    opt.BatchSize = 10;
                })
                .UseExternalBus(
                    new RmqProducerRegistryFactory(
                        connection,
                        [
                            new RmqPublication
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("order-paid"),
                            },
                            new RmqPublication
                            {
                                MakeChannels = OnMissingChannel.Create,
                                Topic = new RoutingKey("order-placed"),
                            },
                        ]
                    ).Create()
                );

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
    string? tmp = Console.ReadLine();

    if (string.IsNullOrEmpty(tmp))
    {
        continue;
    }

    if (tmp == "q")
    {
        break;
    }

    if (!decimal.TryParse(tmp, out decimal value))
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

public class CreateNewOrder() : Command(Guid.NewGuid())
{
    public decimal Value { get; set; }
}

public class OrderPlaced() : Event(Guid.NewGuid())
{
    public string OrderId { get; set; } = string.Empty;
    public decimal Value { get; set; }
}


public class OrderPaid() : Event(Guid.NewGuid())
{
    public string OrderId { get; set; } = string.Empty;
}

public class CreateNewOrderHandler(IAmACommandProcessor commandProcessor,
    IUnitOfWork unitOfWork,
    ILogger<CreateNewOrderHandler> logger) : RequestHandlerAsync<CreateNewOrder>
{
    public override async Task<CreateNewOrder> HandleAsync(CreateNewOrder command, CancellationToken cancellationToken = default)
    {
        await unitOfWork.BeginTransactionAsync();
        try
        {
            var id = Guid.NewGuid().ToString();
            logger.LogInformation("Creating a new order: {OrderId}", id);

            await Task.Delay(10, cancellationToken); // emulating an process

            _ = await commandProcessor.DepositPostAsync(new OrderPlaced { OrderId = id, Value = command.Value }, cancellationToken: cancellationToken);
            if (command.Value % 3 == 0)
            {
                throw new InvalidOperationException("invalid value");
            }

            _ = await commandProcessor.DepositPostAsync(new OrderPaid { OrderId = id }, cancellationToken: cancellationToken);

            await unitOfWork.CommitAsync(cancellationToken);
            return await base.HandleAsync(command, cancellationToken);
        }
        catch
        {
            logger.LogError("Invalid data");
            await unitOfWork.RollbackAsync(cancellationToken);
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

public class OrderPlacedMapper : IAmAMessageMapper<OrderPlaced>
{
    public Message MapToMessage(OrderPlaced request)
    {
        var header = new MessageHeader();
        header.Id = request.Id;
        header.TimeStamp = DateTime.UtcNow;
        header.Topic = "order-placed";
        header.MessageType = MessageType.MT_EVENT;
        header.ReplyTo = "";

        var body = new MessageBody(JsonSerializer.Serialize(request));
        return new Message(header, body);
    }

    public OrderPlaced MapToRequest(Message message)
    {
        return JsonSerializer.Deserialize<OrderPlaced>(message.Body.Bytes)!;
    }
}

public class OrderPaidMapper : IAmAMessageMapper<OrderPaid>
{
    public Message MapToMessage(OrderPaid request)
    {
        var header = new MessageHeader();
        header.Id = request.Id;
        header.TimeStamp = DateTime.UtcNow;
        header.Topic = "order-paid";
        header.MessageType = MessageType.MT_EVENT;
        header.ReplyTo = "";

        var body = new MessageBody(JsonSerializer.Serialize(request));
        return new Message(header, body);
    }

    public OrderPaid MapToRequest(Message message)
    {
        return JsonSerializer.Deserialize<OrderPaid>(message.Body.Bytes)!;
    }
}

public class SqliteConnectionProvider(SqliteUnitOfWork sqlConnection) : ISqliteTransactionConnectionProvider
{
    public SqliteConnection GetConnection()
    {
        return sqlConnection.Connection;
    }

    public Task<SqliteConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(sqlConnection.Connection);
    }

    public SqliteTransaction? GetTransaction()
    {
        return sqlConnection.Transaction;
    }

    public bool HasOpenTransaction => sqlConnection.Transaction != null;
    public bool IsSharedConnection => true;
}

public interface IUnitOfWork
{
    Task BeginTransactionAsync(IsolationLevel isolationLevel = IsolationLevel.Serializable);
    Task CommitAsync(CancellationToken cancellationToken);
    Task RollbackAsync(CancellationToken cancellationToken);
}

public class SqliteUnitOfWork : IUnitOfWork
{
    public SqliteUnitOfWork(SqliteConfiguration configuration)
    {
        Connection = new(configuration.ConnectionString);
        Connection.Open();
    }

    public SqliteConnection Connection { get; }
    public SqliteTransaction? Transaction { get; private set; }

    public async Task BeginTransactionAsync(IsolationLevel isolationLevel = IsolationLevel.Serializable)
    {
        if (Transaction == null)
        {
            Transaction = (SqliteTransaction)await Connection.BeginTransactionAsync(isolationLevel);
        }
    }

    public async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (Transaction != null)
        {
            await Transaction.CommitAsync(cancellationToken);
        }
    }

    public async Task RollbackAsync(CancellationToken cancellationToken)
    {
        if (Transaction != null)
        {
            await Transaction.RollbackAsync(cancellationToken);
        }
    }
}
