using System.Text.Json;
using Amazon;
using Amazon.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessagingGateway.AWSSQS;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Paramore.Brighter.Transforms.Attributes;
using Serilog;
using IRequestContext = Paramore.Brighter.IRequestContext;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();


var host = new HostBuilder()
    .UseSerilog()
    .ConfigureServices((_, services) =>
    {
        var connection = new AWSMessagingGatewayConnection(new BasicAWSCredentials("test", "test"), 
            RegionEndpoint.USEast1,
            cfg => cfg.ServiceURL = "http://localhost:4566");

        services
            .AddHostedService<ServiceActivatorHostedService>()
            .AddServiceActivator(opt =>
            {
                opt.Subscriptions = [
                    new SqsSubscription<Greeting>(
                        "greeting-subscription", // Optional
                        "greeting-queue", // SQS queue name
                        ChannelType.PubSub,
                        "greeting.topic".ToValidSNSTopicName(), // SNS Topic Name
                        bufferSize: 2, 
                        messagePumpType: MessagePumpType.Proactor),
                    new SqsSubscription<Farewell>(
                        new SubscriptionName("farawell-subscription"), // Optional
                        new ChannelName("farewell.queue"), // SQS queue name
                        ChannelType.PointToPoint,
                        new RoutingKey("farewell.queue".ToValidSQSQueueName()), // SNS Topic Name
                        bufferSize: 2,
                        messagePumpType: MessagePumpType.Proactor),
                    new SqsSubscription<SnsFifoEvent>(
                        new SubscriptionName("sns-sample-fifo-subscription"), // Optional
                        new ChannelName("sns-sample-fifo".ToValidSQSQueueName(true)), // SQS queue name
                        ChannelType.PubSub,
                        new RoutingKey("sns-sample-fifo".ToValidSNSTopicName(true)), // SNS Topic Name
                        bufferSize: 2,
                        messagePumpType: MessagePumpType.Proactor,
                        topicAttributes: new SnsAttributes { Type = SqsType.Fifo },
                        queueAttributes: new SqsAttributes(type: SqsType.Fifo)),
                ];
                opt.DefaultChannelFactory= new ChannelFactory(connection);
            })
            .AutoFromAssemblies()
            .UseExternalBus(opt =>
            {
                opt.ProducerRegistry = new CombinedProducerRegistryFactory(
                    new SnsMessageProducerFactory(connection, [
                        new SnsPublication<Greeting>
                        {
                            Topic = "greeting.topic".ToValidSNSTopicName(), 
                            MakeChannels = OnMissingChannel.Create
                        },
                        new SnsPublication<SnsFifoEvent>
                        {
                            Topic = "sns-sample-fifo".ToValidSNSTopicName(true),
                            MakeChannels = OnMissingChannel.Create,
                            TopicAttributes = new SnsAttributes
                            {
                                Type = SqsType.Fifo
                            }
                        }
                    ]),
                    new SqsMessageProducerFactory(connection, [
                        new SqsPublication<Farewell>
                        {
                            ChannelName = "farewell.queue".ToValidSQSQueueName(), 
                            Topic = "farewell.queue".ToValidSQSQueueName(), 
                            MakeChannels = OnMissingChannel.Create
                        }
                        // ,
                        // new SqsPublication<Farewell>
                        // {
                        //     Topic = new RoutingKey("farewell.queue".ToValidSQSQueueName()), 
                        //     MakeChannels = OnMissingChannel.Create,
                        //     QueueAttributes = new SqsAttributes()
                        // }
                    ])
                ).Create();
            });
    })
    .Build();

await host.StartAsync();

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
    await process.PostAsync(new Greeting { Name = name });
    await process.PostAsync(new SnsFifoEvent { Value = name, PartitionValue = "123" });
}

await host.StopAsync();

public class SnsFifoMapper : IAmAMessageMapperAsync<SnsFifoEvent>
{
    public Task<Message> MapToMessageAsync(SnsFifoEvent request, Publication publication,
        CancellationToken cancellationToken = new CancellationToken())
    {
        return Task.FromResult(new Message(new MessageHeader
            {
                MessageId = request.Id,
                Topic = publication.Topic!,
                PartitionKey = request.PartitionValue,
                MessageType = MessageType.MT_EVENT,
                TimeStamp = DateTimeOffset.UtcNow
            }, 
            new MessageBody(JsonSerializer.SerializeToUtf8Bytes(request, JsonSerialisationOptions.Options))));
    }

    public Task<SnsFifoEvent> MapToRequestAsync(Message message, CancellationToken cancellationToken = new CancellationToken())
    {
        return Task.FromResult(JsonSerializer.Deserialize<SnsFifoEvent>(message.Body.Bytes, JsonSerialisationOptions.Options)!);
    }

    public IRequestContext? Context { get; set; }
}


public class SqsFifoMapper : IAmAMessageMapperAsync<SqsFifoEvent>
{
    public Task<Message> MapToMessageAsync(SqsFifoEvent request, Publication publication,
        CancellationToken cancellationToken = new CancellationToken())
    {
        return Task.FromResult(new Message(new MessageHeader
            {
                MessageId = request.Id,
                Topic = publication.Topic!,
                PartitionKey = request.PartitionValue,
                MessageType = MessageType.MT_EVENT,
                TimeStamp = DateTimeOffset.UtcNow
            }, 
            new MessageBody(JsonSerializer.SerializeToUtf8Bytes(request, JsonSerialisationOptions.Options))));
    }

    public Task<SqsFifoEvent> MapToRequestAsync(Message message, CancellationToken cancellationToken = new CancellationToken())
    {
        return Task.FromResult(JsonSerializer.Deserialize<SqsFifoEvent>(message.Body.Bytes, JsonSerialisationOptions.Options)!);
    }

    public IRequestContext? Context { get; set; }
}


public class Greeting() : Event(Guid.CreateVersion7())
{
    public string Name { get; set; } = string.Empty;
}

public class Farewell() : Command(Guid.CreateVersion7())
{
    public string Name { get; set; } = string.Empty;
}

public class SnsFifoEvent() : Event(Guid.CreateVersion7())
{
    public string PartitionValue { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
}


public class SqsFifoEvent() : Event(Guid.CreateVersion7())
{
    public string PartitionValue { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
}

public class GreetingHandler(IAmACommandProcessor processor, ILogger<GreetingHandler> logger) : RequestHandlerAsync<Greeting>
{
    public override async Task<Greeting> HandleAsync(Greeting command, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Hello {Name}", command.Name);
        await processor.PostAsync(new Farewell { Name = command.Name }, cancellationToken: cancellationToken);
        return await base.HandleAsync(command, cancellationToken);
    }
}

public class FarewellHandler(ILogger<FarewellHandler> logger)
    : RequestHandlerAsync<Farewell>
{
    public override Task<Farewell> HandleAsync(Farewell command, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Bye bye {Name}", command.Name);
        return base.HandleAsync(command, cancellationToken);
    }
}

public class SnsFifoHandler(ILogger<SnsFifoHandler> logger)
    : RequestHandlerAsync<SnsFifoEvent>
{
    public override Task<SnsFifoEvent> HandleAsync(SnsFifoEvent command, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("SNS Fifo {Name}", command.Value);
        return base.HandleAsync(command, cancellationToken);
    }
}

public class SqsFifoHandler(ILogger<SqsFifoHandler> logger)
    : RequestHandlerAsync<SqsFifoEvent>
{
    public override Task<SqsFifoEvent> HandleAsync(SqsFifoEvent command, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("SNS Fifo {Name}", command.Value);
        return base.HandleAsync(command, cancellationToken);
    }
}
