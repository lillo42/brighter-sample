// See https://aka.ms/new-console-template for more information
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.AzureServiceBus;
using Paramore.Brighter.MessagingGateway.AzureServiceBus.ClientProvider;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;


Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();


var host = new HostBuilder()
    .UseSerilog()
    .ConfigureServices((_, services) =>
    {
        var connection = new ServiceBusVisualStudioCredentialClientProvider(".servicebus.windows.net");

        services
            .AddHostedService<ServiceActivatorHostedService>()
            .AddServiceActivator(opt =>
            {
                opt.Subscriptions = [
                    new AzureServiceBusSubscription<Greeting>(
                        new SubscriptionName("greeting-subscription"), // Optional
                        new ChannelName("greeting-queue"), // Queue name
                        new RoutingKey("greeting-topic"), // Topic Name
                        subscriptionConfiguration: new AzureServiceBusSubscriptionConfiguration{
                            DeadLetteringOnMessageExpiration = true // Enablin DLQ
                        },
                        bufferSize: 2)
                ];
                opt.ChannelFactory = new AzureServiceBusChannelFactory(new AzureServiceBusConsumerFactory(connection, false));
            })
            .AutoFromAssemblies()
            .UseExternalBus(new AzureServiceBusProducerRegistryFactory(connection, new[]
            {
                new AzureServiceBusPublication
                {
                    Topic = new RoutingKey("greeting-topic"),
                    MakeChannels = OnMissingChannel.Create
                }
            }).Create());
    })

    .Build();

_ = host.RunAsync();


while (true)
{
    await Task.Delay(TimeSpan.FromSeconds(10));
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
