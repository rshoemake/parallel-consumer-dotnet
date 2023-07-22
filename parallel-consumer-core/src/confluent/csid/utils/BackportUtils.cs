using System;
using System.IO;

namespace io.confluent.csid.utils
{
    public static class BackportUtils
    {
        public static long ToSeconds(TimeSpan duration)
        {
            return (long)duration.TotalMilliseconds / 1000;
        }

        public static bool IsEmpty<T>(Optional<T> optional)
        {
            return !optional.IsPresent;
        }

        public static bool HasNo<T>(Optional<T> optional)
        {
            return !optional.IsPresent;
        }

        public static byte[] ReadFully(Stream stream)
        {
            return ReadFully(stream, -1, true);
        }

        public static byte[] ReadFully(Stream stream, int length, bool readAll)
        {
            byte[] output = new byte[0];
            if (length == -1) length = int.MaxValue;
            int pos = 0;
            while (pos < length)
            {
                int bytesToRead;
                if (pos >= output.Length)
                {
                    bytesToRead = Math.Min(length - pos, output.Length + 1024);
                    if (output.Length < pos + bytesToRead)
                    {
                        Array.Resize(ref output, pos + bytesToRead);
                    }
                }
                else
                {
                    bytesToRead = output.Length - pos;
                }
                int cc = stream.Read(output, pos, bytesToRead);
                if (cc < 0)
                {
                    if (readAll && length != int.MaxValue)
                    {
                        throw new EndOfStreamException("Detect premature EOF");
                    }
                    else
                    {
                        if (output.Length != pos)
                        {
                            Array.Resize(ref output, pos);
                        }
                        break;
                    }
                }
                pos += cc;
            }
            return output;
        }
    }
}