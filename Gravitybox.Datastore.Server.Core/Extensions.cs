using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Gravitybox.Datastore.Common;
using System.Collections;
using System.IO.Compression;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Gravitybox.Datastore.Server.Core
{
    public static class Extensions
    {
        public static bool ToBool(this string s)
        {
            if (string.IsNullOrEmpty(s)) return false;
            s = s.ToLower();
            return (s == "true" || s == "1");
        }

        public static bool IsDefault<T>(this T value) where T : struct
        {
            var isDefault = value.Equals(default(T));
            return isDefault;
        }

        public static List<T> ToSqlList<T>(this System.Data.DataSet ds, string field)
        {
            try
            {
                var retval = new List<T>();
                if (ds == null) return retval;
                if (ds.Tables.Count == 0) return retval;
                foreach (System.Data.DataRow dr in ds.Tables[0].Rows)
                {
                    retval.Add((T)dr[field]);
                }
                return retval;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static List<Tuple<T, K>> ToSqlList<T, K>(this System.Data.DataSet ds, string field1, string field2)
        {
            try
            {
                var retval = new List<Tuple<T, K>>();
                if (ds == null) return retval;
                if (ds.Tables.Count == 0) return retval;
                foreach (System.Data.DataRow dr in ds.Tables[0].Rows)
                {
                    retval.Add(new Tuple<T, K>((T)dr[field1], (K)dr[field2]));
                }
                return retval;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static string GetSqlDefinition(this FieldDefinition field)
        {
            var sqlLength = string.Empty;
            if (field.DataType == RepositorySchema.DataTypeConstants.String)
            {
                if (field.Length > 0) sqlLength = $"({field.Length})";
                else sqlLength = "(MAX)";
            }
            return $"[{field.TokenName}] [{field.ToSqlType()}] {sqlLength}";
        }

        public static HashSet<T> ToHash<T>(this IEnumerable<T> list)
        {
            var retval = new HashSet<T>();
            if (list == null) return retval;
            foreach (var item in list)
                retval.Add(item);
            return retval;
        }

        public static string ToSqlDirection(this IFieldSort sort)
        {
            if (sort == null) return string.Empty;
            return sort.SortDirection.ToSqlDirection();
        }

        public static string ToSqlDirection(this SortDirectionConstants sort)
        {
            return sort == SortDirectionConstants.Desc ? "DESC" : "ASC";
        }

        public static string ToSqlType(this IFieldDefinition field)
        {
            if (field == null) return string.Empty;
            return ServerUtilities.GetSqlType(field.DataType);
        }

        public static string ReplaceSqlTicks(this string str)
        {
            return str?.Replace("'", "''");
        }

        /// <summary>
        /// Concatenates the members of a list into a string with a comma as a separator
        /// </summary>
        internal static string ToCommaList<T>(this IEnumerable<T> list)
        {
            return list.ToStringList(",");
        }

        /// <summary>
        /// Concatenates the members of a list into a string with a separator
        /// </summary>
        internal static string ToStringList<T>(this IEnumerable<T> list, string separator)
        {
            if (list == null || !list.Any()) return string.Empty;
            return string.Join(separator, list);
        }

        /// <summary />
        internal static long? ToInt64(this string v)
        {
            if (string.IsNullOrEmpty(v)) return null;
            if (long.TryParse(v, out long parsed))
                return parsed;
            return null;
        }

        /// <summary>
        /// Given a DataItem object this will generate a repeatable has for its state
        /// </summary>
        public static long Hash(this DataItem item)
        {
            if (item == null || item.ItemArray == null) return 0;
            var sb = new StringBuilder();
            foreach (var o in item.ItemArray)
            {
                if (o == null)
                    sb.Append("~~NULL");
                else if (o is string)
                    sb.Append((string)o);
                else if (o is DateTime)
                    sb.Append(((DateTime)o).Ticks);
                else if (o is GeoCode)
                {
                    var g = o as GeoCode;
                    sb.Append(g.Latitude + "*" + g.Longitude);
                }
                else if (o is Array)
                {
                    foreach (var oo in (o as Array))
                    {
                        if (oo == null)
                            sb.Append("~~NULL-Arr");
                        else
                            sb.Append(oo.ToString());
                        sb.Append("|~|");
                    }
                }
                else
                    sb.Append(o.ToString());
                sb.Append("|");
            }
            string data = sb.ToString();

            UInt64 hashedValue = 3074457345618258791ul;
            for (int i = 0; i < data.Length; i++)
            {
                hashedValue += data[i];
                hashedValue *= 3074457345618258799ul;
            }

            //Convert to long as it is just a hash.
            //We do not care what the actual value is as long as it is unique
            return (long)hashedValue;
        }

        internal static bool Match(this string s, string str)
        {
            if (s == null && str == null) return true;
            if (s != null && str == null) return false;
            if (s == null && str != null) return false;
            return string.Equals(s, str, StringComparison.InvariantCultureIgnoreCase);
        }

        /// <summary />
        internal static List<T> ToClone<T>(this IEnumerable<ICloneable> source)
        {
            var retval = new List<T>();
            source.ToList().ForEach(x => retval.Add((T)x.Clone()));
            return retval;
        }

        /// <summary />
        internal static byte[] ObjectToBin(this object obj)
        {
            if (obj == null) throw new Exception("Object cannot be null");
            try
            {
                //Open stream and move to end for writing
                using (var stream = new MemoryStream())
                {
                    stream.Seek(0, SeekOrigin.End);
                    var formatter = new BinaryFormatter();
                    formatter.TypeFormat = System.Runtime.Serialization.Formatters.FormatterTypeStyle.TypesWhenNeeded;
                    formatter.Serialize(stream, obj);
                    stream.Close();
                    var v = stream.ToArray().ZipBytes();
                    return v;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        internal static T BinToObject<T>(this byte[] data)
        {
            try
            {
                if (data == null || !data.Any()) return default(T);
                var formatter = new BinaryFormatter();
                formatter.TypeFormat = System.Runtime.Serialization.Formatters.FormatterTypeStyle.TypesWhenNeeded;
                using (var stream = new MemoryStream(data.UnzipBytes()))
                {
                    return (T)formatter.Deserialize(stream);
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        internal static byte[] Zip(this string value)
        {
            if (string.IsNullOrEmpty(value)) return null;
            var byteArray = System.Text.Encoding.UTF8.GetBytes(value);
            return byteArray.ZipBytes();
        }

        /// <summary />
        internal static byte[] ZipBytes(this byte[] byteArray, bool fast = true)
        {
            if (byteArray == null) return null;
            try
            {
                //Prepare for compress
                using (var ms = new System.IO.MemoryStream())
                {
                    var level = System.IO.Compression.CompressionLevel.Fastest;
                    if (!fast) level = System.IO.Compression.CompressionLevel.Optimal;
                    using (var sw = new System.IO.Compression.GZipStream(ms, level))
                    {
                        //Compress
                        sw.Write(byteArray, 0, byteArray.Length);
                        sw.Close();

                        //Transform byte[] zip data to string
                        return ms.ToArray();
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        internal static string ZipFile(string fileName)
        {
            try
            {
                if (!File.Exists(fileName)) return null;
                var fileToCompress = new FileInfo(fileName);
                var zipFile = fileToCompress.FullName + ".gz";
                if (File.Exists(zipFile)) File.Delete(zipFile);

                using (var originalFileStream = fileToCompress.OpenRead())
                {
                    using (var compressedFileStream = File.Create(zipFile))
                    {
                        using (var compressionStream = new GZipStream(compressedFileStream, CompressionMode.Compress))
                        {
                            originalFileStream.CopyTo(compressionStream);
                        }
                    }
                }
                return zipFile;
            }
            catch (Exception ex)
            {
                //throw;
                return null;
            }
        }

        /// <summary />
        internal static bool UnzipFile(string gzipFile, string newFile)
        {
            try
            {
                if (!File.Exists(gzipFile))
                    return false;

                var fileToDecompress = new FileInfo(gzipFile);
                using (var originalFileStream = fileToDecompress.OpenRead())
                {
                    var currentFileName = fileToDecompress.FullName;
                    using (var decompressedFileStream = File.Create(newFile))
                    {
                        using (GZipStream decompressionStream = new GZipStream(originalFileStream, CompressionMode.Decompress))
                        {
                            decompressionStream.CopyTo(decompressedFileStream);
                            Console.WriteLine("Decompressed: {0}", fileToDecompress.Name);
                        }
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                //throw;
                return false;
            }
        }

        /// <summary />
        internal static string Unzip(this byte[] byteArray)
        {
            //If null stream return null string
            if (byteArray == null) return null;

            //If NOT compressed then return string, no de-compression
            if (byteArray.Length > 3 && (byteArray[0] == 31 && byteArray[1] == 139 && byteArray[2] == 8))
            {
                //Compressed
            }
            else
            {
                var xml = System.Text.Encoding.Unicode.GetString(byteArray);

                // Check for byte order mark
                if (xml.StartsWith("<") || xml[0] == 0xfeff)
                {
                    xml = System.Text.RegularExpressions.Regex.Replace(xml, @"[^\u0000-\u007F]", string.Empty);
                    return xml;
                }
                else
                {
                    return System.Text.Encoding.UTF8.GetString(byteArray);
                }
            }
            return System.Text.Encoding.UTF8.GetString(byteArray.UnzipBytes());
        }

        /// <summary />
        internal static byte[] UnzipBytes(this byte[] byteArray)
        {
            //If null stream return null string
            if (byteArray == null) return null;

            try
            {
                using (var memoryStream = new MemoryStream(byteArray))
                {
                    using (var gZipStream = new GZipStream(memoryStream, CompressionMode.Decompress))
                    {
                        using (var writerStream = new MemoryStream())
                        {
                            gZipStream.CopyTo(writerStream);
                            gZipStream.Close();
                            memoryStream.Close();
                            return writerStream.ToArray();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        internal static bool IsDimensionDump(this DataQuery query)
        {
            if (query == null) return false;
            if (query.IncludeRecords) return false;
            if (!query.IncludeDimensions) return false;
            if (!query.ExcludeDimensionCount) return false;
            if (query.FieldFilters?.Any() == true) return false;
            if (query.FieldSelects?.Any() == true) return false;
            if (query.GroupFields?.Any() == true) return false;
            if (query.DerivedFieldList?.Any() == true) return false;
            if (query.DimensionValueList?.Any() == true) return false;
            return true;
        }
    }
}