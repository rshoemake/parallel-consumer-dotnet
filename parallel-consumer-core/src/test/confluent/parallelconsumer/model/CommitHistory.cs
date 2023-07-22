using System;
using System.Collections.Generic;
using System.Linq;

namespace io.confluent.parallelconsumer.model
{
    public class CommitHistory
    {
        private readonly List<OffsetAndMetadata> history;

        public CommitHistory(List<OffsetAndMetadata> collect)
        {
            this.history = collect;
        }

        public bool Contains(int offset)
        {
            return history.Any(x => x.Offset == offset);
        }

        public Optional<long> HighestCommit()
        {
            Optional<OffsetAndMetadata> last = CollectionUtils.GetLast(history);
            return last.Map(x => x.Offset);
        }

        public List<long> GetOffsetHistory()
        {
            return history.Select(x => x.Offset).ToList();
        }

        public HighestOffsetAndIncompletes GetEncodedSucceeded()
        {
            Optional<OffsetAndMetadata> first = GetHead();
            OffsetAndMetadata offsetAndMetadata = first.Get();
            HighestOffsetAndIncompletes highestOffsetAndIncompletes =
                OffsetMapCodecManager.DeserialiseIncompleteOffsetMapFromBase64(offsetAndMetadata.Offset, offsetAndMetadata.Metadata);
            return highestOffsetAndIncompletes;
        }

        private Optional<OffsetAndMetadata> GetHead()
        {
            Optional<OffsetAndMetadata> first = history.IsEmpty()
                ? Optional.Empty<OffsetAndMetadata>()
                : Optional.Of(history[history.Count - 1]);
            return first;
        }

        public string GetEncoding()
        {
            return GetHead().Get().Metadata;
        }
    }
}