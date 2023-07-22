using System.Collections.Generic;
using System.Linq;

namespace Confluent.Csid.Utils
{
    public static class CSharpStreamUtils
    {
        public static IEnumerable<T> SetupStreamFromDeque<T>(Deque<T> userProcessResultsStream)
        {
            return userProcessResultsStream.ToList();
        }
    }
}