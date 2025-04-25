using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.MsSql;
using Paramore.Brighter.MessagingGateway.MsSql;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;
using Paramore.Brighter.Extensions.DependencyInjection;

Log.Logger = new LoggerConfiguration()
  .MinimumLevel.Information()
  .Enrich.FromLogContext()
  .WriteTo.Console()
  .CreateLogger();

IHost host = new HostBuilder()
  .UseSerilog()
  .ConfigureServices(static (_, services) =>
      {
          MsSqlConfiguration config = new("Server=127.0.0.1,11433;Database=BrighterTests;User Id=sa;Password=Password123!;Application Name=BrighterTests;Connect Timeout=60;Encrypt=false", queueStoreTable: "QueueData");
          services
            .AddHostedService<ServiceActivatorHostedService>()
            .AddServiceActivator(opt =>
            {
                opt.Subscriptions = [
                  new MsSqlSubscription<Greeting>(
                        name: new SubscriptionName("greeting.subscription"),
                        channelName: new ChannelName("greeting.topic"),
                        makeChannels: OnMissingChannel.Create
                      )
                ];

                opt.ChannelFactory = new ChannelFactory(new MsSqlMessageConsumerFactory(config));
            })
            .UseExternalBus(new MsSqlProducerRegistryFactory(config, [
                  new Publication{
                      Topic = new RoutingKey("greeting.topic"),
                      MakeChannels = OnMissingChannel.Create
                   }]).Create())
            .AutoFromAssemblies();
      })
  .Build();

_ = host.StartAsync();

while (true)
{
    await Task.Delay(TimeSpan.FromSeconds(2));
    Console.Write("Say your name (or q to quit): ");
    string? name = Console.ReadLine();

    if (string.IsNullOrEmpty(name))
    {

        continue;
    }

    if (name == "q")
    {
        break;
    }

    IAmACommandProcessor process = host.Services.GetRequiredService<IAmACommandProcessor>();
    process.Post(new Greeting { Name = name });
}

await host.StopAsync();

public class Greeting() : Event(Guid.NewGuid())
{
    public string Name { get; set; } = string.Empty;
}

public class GreetingMapper : IAmAMessageMapper<Greeting>
{
    public Message MapToMessage(Greeting request)
    {
        var header = new MessageHeader();
        header.Id = request.Id;
        header.TimeStamp = DateTime.UtcNow;
        header.Topic = "greeting.topic";
        header.MessageType = MessageType.MT_EVENT;

        var body = new MessageBody(JsonSerializer.Serialize(request));
        return new Message(header, body);
    }

    public Greeting MapToRequest(Message message)
    {
        return JsonSerializer.Deserialize<Greeting>(message.Body.Bytes)!;
    }
}

public class GreetingHandler(ILogger<GreetingHandler> logger) : RequestHandler<Greeting>
{
    public override Greeting Handle(Greeting command)
    {
        logger.LogInformation("Hello {Name}", command.Name);
        return base.Handle(command);
    }
}
