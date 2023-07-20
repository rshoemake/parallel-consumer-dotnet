using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace io.confluent.csid.utils
{
    /*-
     * Copyright (C) 2020-2022 Confluent, Inc.
     */

    using log4net;
    using System.Collections.Concurrent;

    /**
     * Trims long lists to make large List assertions more readable
     */
    public class TrimListRepresentation : StandardRepresentation
    {
        private readonly int sizeLimit = 10;

        protected static string msg = "Collection has been trimmed...";

        public override string ToStringOf(object raw)
        {
            if (raw is HashSet<object>)
            {
                var aSet = (HashSet<object>)raw;
                if (aSet.Count > sizeLimit)
                    raw = new List<object>(aSet);
            }
            if (raw is object[])
            {
                object[] anObjectArray = (object[])raw;
                if (anObjectArray.Length > sizeLimit)
                    raw = anObjectArray.ToList();
            }
            if (raw is string[])
            {
                var anObjectArray = (string[])raw;
                if (anObjectArray.Length > sizeLimit)
                    raw = anObjectArray.ToList();
            }
            if (raw is List<object>)
            {
                List<object> aList = (List<object>)raw;
                if (aList.Count > sizeLimit)
                {
                    LogManager.GetLogger(typeof(TrimListRepresentation)).DebugFormat("List too long ({0}), trimmed...", aList.Count);
                    var trimmedListView = aList.GetRange(0, sizeLimit);
                    // don't mutate backing lists
                    var copy = new ConcurrentBag<object>(trimmedListView);
                    copy.Add(msg);
                    return base.ToStringOf(copy);
                }
            }
            return base.ToStringOf(raw);
        }
    }
}