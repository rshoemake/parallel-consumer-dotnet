using System;
using System.Threading.Tasks;

namespace Confluent.ParallelConsumer.Internal
{
    public interface IOffsetCommitter
    {
        Task RetrieveOffsetsAndCommit();
    }
}