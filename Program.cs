using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.Extensions.Hosting;
using Paramore.Brighter.MessagingGateway.RMQ;
using Paramore.Brighter.MsSql;
using Paramore.Brighter.Outbox.MsSql;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;

const string ConnectionString = "Server=127.0.0.1,1433;Database=BrighterTests;User Id=sa;Password=Password123!;Application Name=BrighterTests;Connect Timeout=60;Encrypt=false;";

using (SqlConnection connection = new("Server=127.0.0.1,1433;Database=master;User Id=sa;Password=Password123!;Application Name=BrighterTests;Connect Timeout=60;Encrypt=false;"))
{
    await connection.OpenAsync();

    using SqlCommand command = connection.CreateCommand();
    command.CommandText =
      """
      IF DB_ID('BrighterTests') IS NULL
      BEGIN
        CREATE DATABASE BrighterTests;
      END;
      """;
    _ = await command.ExecuteNonQueryAsync();

}

using (SqlConnection connection = new(ConnectionString))
{
    await connection.OpenAsync();

    using SqlCommand command = connection.CreateCommand();
    command.CommandText =
      """
      IF OBJECT_ID('OutboxMessages', 'U') IS NULL
      BEGIN 
        CREATE TABLE [OutboxMessages]
        (
          [Id] [BIGINT] NOT NULL IDENTITY ,
          [MessageId] UNIQUEIDENTIFIER NOT NULL ,
          [Topic] NVARCHAR(255) NULL ,

          [MessageType] NVARCHAR(32) NULL ,
          [Timestamp] DATETIME NULL ,
          [CorrelationId] UNIQUEIDENTIFIER NULL,

          [ReplyTo] NVARCHAR(255) NULL,
          [ContentType] NVARCHAR(128) NULL,  
          [Dispatched] DATETIME NULL,
          [HeaderBag] NTEXT NULL ,
          [Body] NTEXT NULL ,
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
              .AddScoped<SqlUnitOfWork, SqlUnitOfWork>()
              .TryAddScoped<IUnitOfWork>(provider => provider.GetRequiredService<SqlUnitOfWork>());

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
                .UseMsSqlOutbox(new MsSqlConfiguration(ConnectionString, "OutboxMessages"), typeof(SqlConnectionProvider), ServiceLifetime.Scoped)
                .UseMsSqlTransactionConnectionProvider(typeof(SqlConnectionProvider))
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
                                Topic = new RoutingKey("greeting.topic"),
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
        using IServiceScope scope = host.Services.CreateScope();
        IAmACommandProcessor process = scope.ServiceProvider.GetRequiredService<IAmACommandProcessor>();
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
        await unitOfWork.BeginTransactionAsync(cancellationToken);
        try
        {
            string id = Guid.NewGuid().ToString();
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

        var body = new MessageBody(JsonSerializer.Serialize(request));
        return new Message(header, body);
    }

    public OrderPaid MapToRequest(Message message)
    {
        return JsonSerializer.Deserialize<OrderPaid>(message.Body.Bytes)!;
    }
}

public class SqlConnectionProvider(SqlUnitOfWork sqlConnection) : IMsSqlTransactionConnectionProvider
{
    private readonly SqlUnitOfWork _sqlConnection = sqlConnection;

    public SqlConnection GetConnection()
    {
        return _sqlConnection.Connection;
    }

    public Task<SqlConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_sqlConnection.Connection);
    }

    public SqlTransaction? GetTransaction()
    {
        return _sqlConnection.Transaction;
    }

    public bool HasOpenTransaction => _sqlConnection.Transaction != null;
    public bool IsSharedConnection => true;
}

public interface IUnitOfWork
{
    Task BeginTransactionAsync(CancellationToken cancellationToken, IsolationLevel isolationLevel = IsolationLevel.Serializable);
    Task CommitAsync(CancellationToken cancellationToken);
    Task RollbackAsync(CancellationToken cancellationToken);
}

public class SqlUnitOfWork(MsSqlConfiguration configuration) : IUnitOfWork
{
    public SqlConnection Connection { get; } = new(configuration.ConnectionString);
    public SqlTransaction? Transaction { get; private set; }

    public async Task BeginTransactionAsync(CancellationToken cancellationToken,
        IsolationLevel isolationLevel = IsolationLevel.Serializable)
    {

        if (Transaction == null)
        {
            if (Connection.State != ConnectionState.Open)
            {
                await Connection.OpenAsync(cancellationToken);
            }

            Transaction = Connection.BeginTransaction(isolationLevel);
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

    public async Task<SqlCommand> CreateSqlCommandAsync(string sql, SqlParameter[] parameters, CancellationToken cancellationToken)
    {
        if (Connection.State != ConnectionState.Open)
        {
            await Connection.OpenAsync(cancellationToken);
        }

        SqlCommand command = Connection.CreateCommand();

        if (Transaction != null)
        {
            command.Transaction = Transaction;
        }

        command.CommandText = sql;
        if (parameters.Length > 0)
        {
            command.Parameters.AddRange(parameters);
        }

        return command;
    }
}
