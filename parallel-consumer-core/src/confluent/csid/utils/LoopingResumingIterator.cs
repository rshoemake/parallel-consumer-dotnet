using System;
using System.Collections.Generic;

namespace io.confluent.csid.utils
{
    public class LoopingResumingIterator<KEY, VALUE>
    {
        private Optional<Map.Entry<KEY, VALUE>> head = Optional.Empty;

        private IEnumerator<Map.Entry<KEY, VALUE>> wrappedIterator;

        private readonly long iterationTargetCount;

        private long iterationCount = 0;

        public Optional<KEY> IterationStartingPointKey { get; }

        private readonly Dictionary<KEY, VALUE> map;

        private bool isOnSecondPass = false;

        private bool terminalState = false;

        private bool startingPointKeyValid = false;

        public static LoopingResumingIterator<KKEY, VVALUE> Build<KKEY, VVALUE>(KKEY startingKey, Dictionary<KKEY, VVALUE> map)
        {
            return new LoopingResumingIterator<KKEY, VVALUE>(Optional.OfNullable(startingKey), map);
        }

        public LoopingResumingIterator(Optional<KEY> startingKey, Dictionary<KEY, VALUE> map)
        {
            IterationStartingPointKey = startingKey;
            this.map = map;
            wrappedIterator = map.GetEnumerator();
            iterationTargetCount = map.Count;

            if (startingKey.IsPresent)
            {
                head = AdvanceToStartingPointAndGet(startingKey.Get());
                if (head.IsPresent)
                {
                    startingPointKeyValid = true;
                }
                else
                {
                    ResetIteratorToZero();
                }
            }
        }

        public LoopingResumingIterator(Dictionary<KEY, VALUE> map) : this(Optional.Empty<KEY>(), map)
        {
        }

        public Optional<Map.Entry<KEY, VALUE>> Next()
        {
            iterationCount++;

            if (terminalState)
            {
                return Optional.Empty<Map.Entry<KEY, VALUE>>();
            }
            else if (head.IsPresent)
            {
                Optional<Map.Entry<KEY, VALUE>> headSave = TakeHeadValue();
                return headSave;
            }

            if (wrappedIterator.MoveNext())
            {
                Map.Entry<KEY, VALUE> next = wrappedIterator.Current;
                bool onSecondPassAndReachedStartingPoint = IterationStartingPointKey.Equals(Optional.Of(next.Key));
                bool numberElementsReturnedExceeded = iterationCount > iterationTargetCount + 1;
                if (onSecondPassAndReachedStartingPoint || numberElementsReturnedExceeded)
                {
                    terminalState = true;
                    return Optional.Empty<Map.Entry<KEY, VALUE>>();
                }
                else
                {
                    return Optional.OfNullable(next);
                }
            }
            else if (IterationStartingPointKey.IsPresent && startingPointKeyValid && !isOnSecondPass)
            {
                ResetIteratorToZero();
                isOnSecondPass = true;
                return Next();
            }
            else
            {
                return Optional.Empty<Map.Entry<KEY, VALUE>>();
            }
        }

        private Optional<Map.Entry<KEY, VALUE>> TakeHeadValue()
        {
            var headSave = head;
            head = Optional.Empty<Map.Entry<KEY, VALUE>>();
            return headSave;
        }

        private Optional<Map.Entry<KEY, VALUE>> AdvanceToStartingPointAndGet(object startingPointObject)
        {
            while (wrappedIterator.MoveNext())
            {
                Map.Entry<KEY, VALUE> next = wrappedIterator.Current;
                if (next.Key.Equals(startingPointObject))
                {
                    return Optional.Of(next);
                }
            }
            return Optional.Empty<Map.Entry<KEY, VALUE>>();
        }

        private void ResetIteratorToZero()
        {
            wrappedIterator = map.GetEnumerator();
        }
    }
}