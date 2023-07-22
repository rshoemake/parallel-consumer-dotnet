using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using System.IO.Compression;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace OffsetSimpleSerialisation
{
    public static class OffsetSimpleSerialisation
    {
        public static string EncodeAsJavaObjectStream(HashSet<long> incompleteOffsets)
        {
            using (var memoryStream = new MemoryStream())
            {
                var binaryFormatter = new BinaryFormatter();
                binaryFormatter.Serialize(memoryStream, incompleteOffsets);
                return Convert.ToBase64String(memoryStream.ToArray());
            }
        }

        private static HashSet<long> DeserialiseJavaWriteObject(byte[] decode)
        {
            using (var memoryStream = new MemoryStream(decode))
            {
                var binaryFormatter = new BinaryFormatter();
                return (HashSet<long>)binaryFormatter.Deserialize(memoryStream);
            }
        }

        public static byte[] CompressSnappy(byte[] bytes)
        {
            using (var outStream = new MemoryStream())
            {
                using (var snappyStream = new SnappyStream(outStream, CompressionMode.Compress))
                {
                    snappyStream.Write(bytes, 0, bytes.Length);
                }
                return outStream.ToArray();
            }
        }

        public static byte[] DecompressSnappy(byte[] input)
        {
            using (var inStream = new MemoryStream(input))
            {
                using (var snappyStream = new SnappyStream(inStream, CompressionMode.Decompress))
                {
                    using (var outStream = new MemoryStream())
                    {
                        snappyStream.CopyTo(outStream);
                        return outStream.ToArray();
                    }
                }
            }
        }

        public static string Base64(byte[] src)
        {
            return Convert.ToBase64String(src);
        }

        public static byte[] CompressZstd(byte[] bytes)
        {
            using (var outStream = new MemoryStream())
            {
                using (var zstdStream = new ZstdOutputStream(outStream))
                {
                    zstdStream.Write(bytes, 0, bytes.Length);
                }
                return outStream.ToArray();
            }
        }

        public static byte[] CompressGzip(byte[] bytes)
        {
            using (var outStream = new MemoryStream())
            {
                using (var gzipStream = new GZipStream(outStream, CompressionMode.Compress))
                {
                    gzipStream.Write(bytes, 0, bytes.Length);
                }
                return outStream.ToArray();
            }
        }

        public static byte[] DecodeBase64(string b64)
        {
            return Convert.FromBase64String(b64);
        }

        public static byte[] DecompressZstd(byte[] input)
        {
            using (var inStream = new MemoryStream(input))
            {
                using (var zstdStream = new ZstdInputStream(inStream))
                {
                    using (var outStream = new MemoryStream())
                    {
                        zstdStream.CopyTo(outStream);
                        return outStream.ToArray();
                    }
                }
            }
        }

        public static byte[] DecompressGzip(byte[] input)
        {
            using (var inStream = new MemoryStream(input))
            {
                using (var gzipStream = new GZipStream(inStream, CompressionMode.Decompress))
                {
                    using (var outStream = new MemoryStream())
                    {
                        gzipStream.CopyTo(outStream);
                        return outStream.ToArray();
                    }
                }
            }
        }

        public static string DeserialiseByteArrayToBitMapString(byte[] data)
        {
            StringBuilder sb = new StringBuilder(data.Length);
            foreach (byte b in data)
            {
                if (b == 1)
                {
                    sb.Append('x');
                }
                else
                {
                    sb.Append('o');
                }
            }
            return sb.ToString();
        }
    }
}