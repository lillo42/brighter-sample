using Paramore.Brighter;

namespace brighter_sample;

public class Greeting() : Event(Id.Random())
{
    public string Name { get; set; } = string.Empty;
}
