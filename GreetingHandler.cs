using Paramore.Brighter;
using Paramore.Brighter.Actions;
using Paramore.Brighter.Policies.Attributes;

namespace brighter_sample;

public class GreetingHandler : RequestHandler<Greeting>
{
    [FallbackPolicy(true, false, 0)]
    public override Greeting Handle(Greeting @event)
    {
        if (@event.Name == "fail")
        {
            Console.WriteLine("===== fail to process");
            throw new Exception("Some error");
        }
        
        Console.WriteLine("===== Hello, {0}", @event.Name);
        return base.Handle(@event);
    }

    private static int _counter = 0;
    public override Greeting Fallback(Greeting command)
    {
        var res = Interlocked.Increment(ref _counter);
        if (res % 3 == 0)
        {
            Console.Write("=== marking as completed");
            return command;
        }
        
        Console.Write("=== fallback rethrowing");
        throw new DeferMessageAction();
    }
}