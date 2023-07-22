using System;
using System.Collections.Generic;
using System.Text;

namespace io.confluent.csid.utils
{
    public static class StringTestUtils
    {
        public static readonly StandardRepresentation STANDARD_REPRESENTATION = new StandardRepresentation();

        public static string Pretty(object properties)
        {
            return STANDARD_REPRESENTATION.ToStringOf(properties);
        }
    }
}