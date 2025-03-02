using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;

Console.WriteLine("Hello, World!");

var host = Host.CreateDefaultBuilder()
.ConfigureServices(static (_, services) =>
{
    services.AddBrighter()
          .AutoFromAssemblies();
}).Build();

var command = host.Services.GetRequiredService<IAmACommandProcessor>();

command.Send(new MyCommand { Text = Guid.NewGuid().ToString("N") });
command.Publish(new MyEvent { Text = Guid.NewGuid().ToString("N") });

public class MyCommand() : Command(Guid.NewGuid())
{
    public string Text { get; set; } = string.Empty;
}

public class MyCommandHandler(ILogger<MyCommandHandler> logger) : RequestHandler<MyCommand>
{
    public override MyCommand Handle(MyCommand command)
    {
        logger.LogInformation("Received text: {Text}", command.Text);
        return base.Handle(command);
    }
}


public class MyEvent() : Event(Guid.NewGuid())
{
    public string Text { get; set; } = string.Empty;
}

public class MyEventHandler1(ILogger<MyEventHandler1> logger) : RequestHandler<MyEvent>
{
    public override MyEvent Handle(MyEvent command)
    {
        logger.LogInformation("Received text(from handler 1): {Text}", command.Text);
        return base.Handle(command);
    }
}

public class MyEventHandler2(ILogger<MyEventHandler2> logger) : RequestHandler<MyEvent>
{
    public override MyEvent Handle(MyEvent command)
    {
        logger.LogInformation("Received text(from handler 2): {Text}", command.Text);
        return base.Handle(command);
    }
}
