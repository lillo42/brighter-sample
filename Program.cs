using System.Data;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.Extensions.Hosting;
using Paramore.Brighter.MessagingGateway.RMQ;
using Paramore.Brighter.Outbox.PostgreSql;
using Paramore.Brighter.PostgreSql;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;

const string ConnectionString = "Host=localhost;Username=postgres;Password=password;Database=brightertests;";
await using (NpgsqlConnection connection = new(ConnectionString))
{
    await connection.OpenAsync();
    await using NpgsqlCommand command = connection.CreateCommand();
    command.CommandText =
      """
      CREATE TABLE IF NOT EXISTS "outboxmessages"
      (
        "id" BIGSERIAL NOT NULL,
        "messageid" UUID NOT NULL,
        "topic" VARCHAR(255) NULL,
        "messagetype" VARCHAR(32) NULL,
        "timestamp" TIMESTAMP NULL,
        "correlationid" UUID NULL,
        "replyto" VARCHAR(255) NULL,
        "contenttype" VARCHAR(128) NULL,
        "dispatched" TIMESTAMP NULL,
        "headerbag" TEXT NULL,
        "body" TEXT NULL,
        PRIMARY KEY (Id)
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
                .UsePostgreSqlOutbox(new PostgreSqlOutboxConfiguration(ConnectionString, "OutboxMessages"))
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
    ILogger<CreateNewOrderHandler> logger) : RequestHandlerAsync<CreateNewOrder>
{
    public override async Task<CreateNewOrder> HandleAsync(CreateNewOrder command, CancellationToken cancellationToken = default)
    {
        try
        {
            string id = Guid.NewGuid().ToString();
            logger.LogInformation("Creating a new order: {OrderId}", id);

            await Task.Delay(10, cancellationToken); // emulating an process

            _ = commandProcessor.DepositPost(new OrderPlaced { OrderId = id, Value = command.Value });
            if (command.Value % 3 == 0)
            {
                throw new InvalidOperationException("invalid value");
            }

            _ = commandProcessor.DepositPost(new OrderPaid { OrderId = id });

            return await base.HandleAsync(command, cancellationToken);
        }
        catch (Exception ex)
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
