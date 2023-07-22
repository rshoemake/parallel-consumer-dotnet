using System;

namespace io.confluent.csid.utils
{
    public static class StringUtils
    {
        public static string Msg(string s, params object[] args)
        {
            return string.Format(s, args);
        }

        public static bool IsBlank(string property)
        {
            if (property == null) return true;
            else return property.Trim().Length == 0;
        }
    }
}