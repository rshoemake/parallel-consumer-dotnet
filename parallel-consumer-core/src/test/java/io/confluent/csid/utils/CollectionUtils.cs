using System.Collections.Generic;

namespace Confluent.Csid.Utils
{
    public static class CollectionUtils
    {
        public static Optional<T> GetLast<T>(List<T> history)
        {
            return history.Count == 0 ? Optional<T>.Empty : Optional<T>.Of(history[history.Count - 1]);
        }
    }
}