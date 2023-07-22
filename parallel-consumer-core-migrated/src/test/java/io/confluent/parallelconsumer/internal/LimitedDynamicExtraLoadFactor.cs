namespace Confluent.ParallelConsumer.Internal
{
    public class LimitedDynamicExtraLoadFactor : DynamicLoadFactor
    {
        public override int GetMaxFactor()
        {
            return 2;
        }

        public override int GetCurrentFactor()
        {
            return 2;
        }
    }
}