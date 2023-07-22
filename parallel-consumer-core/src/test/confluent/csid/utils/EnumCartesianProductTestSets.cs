using System;

namespace io.confluent.csid.utils
{
    /*-
     * Copyright (C) 2020-2021 Confluent, Inc.
     */

    public class EnumCartesianProductTestSets : CartesianProductTest.Sets
    {
        /**
         * Simply pass in the enum class, otherwise use as normal.
         *
         * @see CartesianProductTest.Sets#add
         */
        public override CartesianProductTest.Sets Add(params object[] entries)
        {
            object[] finalEntries = entries;
            if (entries.Length == 1)
            {
                object entry = entries[0];
                if (entry is Type)
                {
                    Type classEntry = (Type)entry;
                    if (classEntry.IsEnum)
                    {
                        finalEntries = Enum.GetValues(classEntry);
                    }
                }
            }
            return base.Add(finalEntries);
        }
    }
}