using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.MessagingGateway.MsSql;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MsSql;

Log.Logger = new LoggerConfiguration()
  .MinimumLevel.Information()
  .Enrich.FromLogContext()
  .WriteTo.Console()
  .CreateLogger();


await using (SqlConnection connection = new("Server=127.0.0.1,11433;Database=master;User Id=sa;Password=Password123!;Application Name=BrighterTests;Connect Timeout=60;Encrypt=false;"))
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

await using (SqlConnection connection = new("Server=127.0.0.1,11433;Database=BrighterTests;User Id=sa;Password=Password123!;Application Name=BrighterTests;Connect Timeout=60;Encrypt=false;"))
{
    await connection.OpenAsync();

    await using var command = connection.CreateCommand();
    command.CommandText =
        """
        IF NOT (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = 'QueueData'))
        BEGIN
            CREATE TABLE [dbo].[QueueData](
                [Id] [bigint] IDENTITY(1,1) NOT NULL,
                [Topic] [nvarchar](255) NOT NULL,
                [MessageType] [nvarchar](1024) NOT NULL,
                [Payload] [nvarchar](max) NOT NULL,
                CONSTRAINT [PK_QueueData] PRIMARY KEY CLUSTERED ([Id] ASC)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
            ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
            
            CREATE NONCLUSTERED INDEX [IX_Topic] ON [dbo].[QueueData]([Topic] ASC) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        END;
        """;
    _ = await command.ExecuteNonQueryAsync();

}

var host = new HostBuilder()
  .UseSerilog()
  .ConfigureServices(static (_, services) =>
      {
          var config = new RelationalDatabaseConfiguration ("Server=127.0.0.1,11433;Database=BrighterTests;User Id=sa;Password=Password123!;Application Name=BrighterTests;Connect Timeout=60;Encrypt=false", queueStoreTable: "QueueData");
          services
            .AddHostedService<ServiceActivatorHostedService>()
            .AddServiceActivator(opt =>
            {
                opt.Subscriptions = [
                  new MsSqlSubscription<Greeting>(
                        subscriptionName: new SubscriptionName("greeting.subscription"),
                        channelName: new ChannelName("greeting.topic"),
                        makeChannels: OnMissingChannel.Create,
                        messagePumpType: MessagePumpType.Reactor,
                        timeOut:  TimeSpan.FromSeconds(10)
                      )
                ];

                opt.DefaultChannelFactory= new ChannelFactory(new MsSqlMessageConsumerFactory(config));
            })
            .UseExternalBus(opt =>
            {
                opt.ProducerRegistry = new MsSqlProducerRegistryFactory(config, [
                    new Publication<Greeting>
                    {
                        Topic = new RoutingKey("greeting.topic"),
                        MakeChannels = OnMissingChannel.Create
                    }
                ]).Create();
            })
            .AutoFromAssemblies();
      })
  .Build();

await host.StartAsync();

while (true)
{
    await Task.Delay(TimeSpan.FromSeconds(2));
    Console.Write("Say your name (or q to quit): ");
    var name = Console.ReadLine();

    if (string.IsNullOrEmpty(name))
    {

        continue;
    }

    if (name == "q")
    {
        break;
    }

    var process = host.Services.GetRequiredService<IAmACommandProcessor>();
    process.Post(new Greeting { Name = name });
}

await host.StopAsync();

public class Greeting() : Event(Guid.NewGuid())
{
    public string Name { get; set; } = string.Empty;
}

public class GreetingHandler(ILogger<GreetingHandler> logger) : RequestHandler<Greeting>
{
    public override Greeting Handle(Greeting command)
    {
        logger.LogInformation("Hello {Name}", command.Name);
        return base.Handle(command);
    }
}
