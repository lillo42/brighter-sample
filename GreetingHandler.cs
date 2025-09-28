using Paramore.Brighter;

namespace brighter_sample;

public class GreetingHandler : RequestHandler<Greeting>
{
    public override Greeting Handle(Greeting @event)
    {
        Console.WriteLine("===== Hello, {0}", @event.Name);
        return base.Handle(@event);
    }
}