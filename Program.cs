using brighter_sample;
using Fluent.Brighter;
using Fluent.Brighter.RocketMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Org.Apache.Rocketmq;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.RocketMQ;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureServices(service =>
    {
        // var connection = new RocketMessagingGatewayConnection(new ClientConfig.Builder()
        //     .SetEndpoints("localhost:8081")
        //     .EnableSsl(false)
        //     .SetRequestTimeout(TimeSpan.FromSeconds(10))
        //     .Build());
        //
        // service
        //     .AddHostedService<ServiceActivatorHostedService>()
        //     .AddConsumers(consumer =>
        //     {
        //         consumer.Subscriptions = [
        //             new RocketMqSubscription<Greeting>(
        //                 subscriptionName: "greeting-sub",
        //                 channelName: "greeting-channel",
        //                 routingKey: "greeting",
        //                 messagePumpType: MessagePumpType.Reactor)
        //         ];
        //         consumer.DefaultChannelFactory =
        //             new RocketMqChannelFactory(new RocketMessageConsumerFactory(connection));
        //     })
        //     .AddProducers(producer =>
        //     {
        //         producer.ProducerRegistry = new ProducerRegistry(new RocketMessageProducerFactory(connection, [
        //             new RocketMqPublication<Greeting>
        //             {
        //                 Topic = "greeting"
        //             }
        //         ]).Create());
        //     })
        //     .AutoFromAssemblies();
        
        service
            .AddHostedService<ServiceActivatorHostedService>()
            .AddFluentBrighter(brighter => brighter
                .UsingRocketMq(rocket => rocket
                    .SetConnection(conn => conn
                        .SetClient(c => c
                            .SetEndpoints("localhost:8081")
                            .EnableSsl(false)
                            .SetRequestTimeout(TimeSpan.FromSeconds(10))
                        ))
                    .UsePublications(pub => pub
                        .AddPublication<Greeting>(p => p
                            .SetTopic("greeting")))
                    .UseSubscriptions(sub => sub
                        .AddSubscription<Greeting>(s => s
                            .SetSubscriptionName("greeting-sub-name")
                            .SetTopic("greeting")
                            .SetConsumerGroup("greeting-consumer-group")
                            .UseReactorMode()
                        ))));

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
    await process.PostAsync(new Greeting {Name = name });
}


await host.StopAsync();