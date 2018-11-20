#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using Gravitybox.Datastore.Common.Queryable;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Linq.Expressions;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public static class Extensions
    {
        /// <summary>
        /// Gets a refinement value from dimension refinement item, if it exists
        /// </summary>
        /// <param name="dItem">The dimension on which this refinement resides</param>
        /// <param name="dvidx">The unique identifier of the refinement</param>
        /// <returns></returns>
        public static string GetRefinementValue(this DimensionItem dItem, long dvidx)
        {
            if (dItem != null)
            {
                var rItem = dItem.RefinementList.FirstOrDefault(x => x.DVIdx == dvidx);
                if (rItem != null) return rItem.FieldValue;
            }
            return string.Empty;
        }

        /// <summary>
        /// Gets the dimenion item on which the specified refinement exists
        /// </summary>
        /// <param name="dimensionList">A list of dimensions to check</param>
        /// <param name="dvidx">The unique identifier of the refinement</param>
        /// <returns></returns>
        public static DimensionItem GetDimensionByDVIdx(this IEnumerable<DimensionItem> dimensionList, long dvidx)
        {
            foreach (var dItem in dimensionList)
            {
                var rItem = dItem.RefinementList.FirstOrDefault(x => x.DVIdx == dvidx);
                if (rItem != null) return dItem;
            }
            return null;
        }

        /// <summary>
        /// Gets the dimenion item on which the specified refinement value exists
        /// </summary>
        /// <param name="dimensionList"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static DimensionItem GetDimensionByRefinementValue(this IEnumerable<DimensionItem> dimensionList, string value)
        {
            if (dimensionList == null) return null;
            foreach (var dItem in dimensionList)
            {
                var rItem = dItem.RefinementList.FirstOrDefault(x => x.FieldValue == value);
                if (rItem != null) return dItem;
            }
            return null;
        }

        /// <summary />
        public static IRefinementItem GetRefinementByValue(this DimensionItem dimension, string value)
        {
            if (dimension == null) return null;
            if (value == null) return null;
            var rItem = dimension.RefinementList.FirstOrDefault(x => x.FieldValue == value);
            if (rItem != null) return rItem;
            return null;
        }

        /// <summary />
        public static IRefinementItem GetRefinementByMinValue(this DimensionItem dimension, long value)
        {
            if (dimension == null) return null;
            var rItem = dimension.RefinementList.FirstOrDefault(x => x.MinValue == value);
            if (rItem != null) return rItem;
            return null;
        }

        /// <summary>
        /// Given a refinement value find the first associated refinement item and returns its unique DVIdx
        /// </summary>
        /// <param name="dimension"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static long? GetDVIdxByValue(this DimensionItem dimension, string value)
        {
            if (dimension == null) return null;
            if (value == null) return null;
            var rItem = dimension.RefinementList.FirstOrDefault(x => x.FieldValue == value);
            if (rItem != null) return rItem.DVIdx;
            return null;
        }

        /// <summary>
        /// Returns a refinement item from a list of dimenions based on the unqiue dimension value index
        /// </summary>
        /// <param name="dimensionList"></param>
        /// <param name="dvidx"></param>
        /// <returns></returns>
        public static IRefinementItem GetRefinementByDVIdx(this IEnumerable<DimensionItem> dimensionList, long dvidx)
        {
            foreach (var dItem in dimensionList)
            {
                var rItem = dItem.RefinementList.FirstOrDefault(x => x.DVIdx == dvidx);
                if (rItem != null) return rItem;
            }
            return null;
        }

        /// <summary>
        /// Trims the value parameter off both ends of a string
        /// </summary>
        internal static string Trim(this string s, string value)
        {
            if (string.IsNullOrEmpty(s)) return s;
            if (string.IsNullOrEmpty(value)) return s;

            while (s.StartsWith(value))
            {
                s = s.Substring(value.Length, s.Length - value.Length);
            }

            while (s.EndsWith(value))
            {
                s = s.Substring(0, s.Length - value.Length);
            }

            return s;
        }

        /// <summary />
        internal static string ToXml(object obj)
        {
            using (var writer = new StringWriter())
            {
                var settings = new XmlWriterSettings();
                settings.OmitXmlDeclaration = true;
                settings.Indent = true;
                settings.IndentChars = "\t";
                var xmlWriter = XmlWriter.Create(writer, settings);

                var ns = new XmlSerializerNamespaces();
                ns.Add(string.Empty, "http://www.w3.org/2001/XMLSchema-instance");
                ns.Add(string.Empty, "http://www.w3.org/2001/XMLSchema");

                var serializer = new XmlSerializer(obj.GetType());
                serializer.Serialize(xmlWriter, obj, ns);
                return writer.ToString();
            }
        }

        /// <summary />
        internal static object FromXml(string s, System.Type type)
        {
            var reader = new StringReader(s);
            var serializer = new XmlSerializer(type);
            var xmlReader = new XmlTextReader(reader);
            return serializer.Deserialize(xmlReader);
        }

        /// <summary />
        public static string ToDatastoreDateString(this DateTime? d)
        {
            if (d == null) return string.Empty;
            return d.Value.ToString(DimensionItem.DateTimeFormat);
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

        #region versionConfigToNamespaceAssemblyObjectBinder
        private sealed class versionConfigToNamespaceAssemblyObjectBinder : System.Runtime.Serialization.SerializationBinder
        {
            public override Type BindToType(string assemblyName, string typeName)
            {
                Type typeToDeserialize = null;
                try
                {
                    var ToAssemblyName = assemblyName.Split(',')[0];
                    var assemblies = AppDomain.CurrentDomain.GetAssemblies();
                    foreach (var ass in assemblies)
                    {
                        if (ass.FullName.Split(',')[0] == ToAssemblyName)
                        {
                            typeToDeserialize = ass.GetType(typeName);
                            break;
                        }
                    }
                }
                catch (System.Exception ex)
                {
                    throw;
                }
                return typeToDeserialize;
            }
        }
        #endregion

        ///// <summary />
        //public static void Using<T>(this T client, Action<T> work)
        //    where T : System.ServiceModel.ICommunicationObject
        //{
        //    try
        //    {
        //        work(client);
        //        client.Close();
        //    }
        //    catch (System.ServiceModel.EndpointNotFoundException e)
        //    {
        //        throw;
        //    }
        //    catch (System.ServiceModel.FaultException e)
        //    {
        //        client.Abort();
        //        if (e.Message.Contains((typeof(Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException)).FullName))
        //            throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException();
        //        throw;
        //    }
        //    catch (System.ServiceModel.CommunicationException e)
        //    {
        //        client.Abort();
        //    }
        //    catch (TimeoutException e)
        //    {
        //        client.Abort();
        //    }
        //    catch (Exception e)
        //    {
        //        client.Abort();
        //        throw;
        //    }
        //}

        /// <summary />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool Match(this string s, string str)
        {
            if (s == null && str == null) return true;
            if (s != null && str == null) return false;
            if (s == null && str != null) return false;
            return string.Equals(s, str, StringComparison.InvariantCultureIgnoreCase);
        }

        /// <summary />
        public static RepositorySchema Merge(this RepositorySchema parentSchema, RepositorySchema schema)
        {
            try
            {
                //Create new schema init to extension
                var retval = new RepositorySchema();
                retval.LoadXml(schema.ToXml());

                //Load extension schema, check for errors
                var index = 0;
                foreach (var f in parentSchema.FieldList)
                {
                    if (retval.FieldList.Any(x => x.Name.Match(f.Name)))
                        throw new Exception("Field is already defined in the parent object");
                    else
                        retval.FieldList.Insert(index++, f);
                }
                return retval;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        /// <summary />
        public static RepositorySchema Subtract(this RepositorySchema schema1, RepositorySchema schema2)
        {
            try
            {
                //Create new schema init to parent
                var retval = new RepositorySchema();
                retval.LoadXml(schema1.ToXml());

                //Load extension schema, check for errors
                foreach (var f in schema2.FieldList)
                {
                    if (retval.FieldList.Any(x => x.Name.Match(f.Name)))
                        retval.FieldList.RemoveAll(x => x.Name.Match(f.Name));
                }

                return retval;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        /// <summary>
        /// Break a list of items into chunks of a specific size
        /// </summary>
        public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> source, int chunksize)
        {
            while (source.Any())
            {
                yield return source.Take(chunksize);
                source = source.Skip(chunksize);
            }
        }

        /// <summary />
        public static List<T> ToList<T>(this System.Collections.ICollection list)
        {
            var retval = new List<T>();
            foreach (T item in list)
                retval.Add(item);
            return retval;
        }

        /// <summary />
        internal static int ToInt(this string v)
        {
            if (string.IsNullOrEmpty(v)) return 0;
            int parsed;
            int.TryParse(v, out parsed);
            return parsed;
        }

        /// <summary />
        internal static double ToDouble(this string v)
        {
            double parsed;
            double.TryParse(v, out parsed);
            return parsed;
        }

        /// <summary />
        internal static string ToXmlValue(this string str)
        {
            if (string.IsNullOrEmpty(str)) return str;
            if (!str.Contains(",") && !str.Contains("\"")) return str;
            return string.Format("\"{0}\"", str.Replace("\"", "\"\""));
        }

        internal static int IndexOf<T>(this IEnumerable<T> source, Func<T, bool> predicate)
        {
            int index = 0;
            foreach (var item in source)
            {
                if (predicate(item)) return index;
                index++;
            }
            return -1;
        }

        #region These are templates for Intellisense when using the Datastore lambda for Grouping

        /// <summary />
        public static TReturn Max<T, TType, TReturn>(this IDatastoreGrouping<TType, T> source, Func<T, TReturn> selector)
        {
            throw new NotImplementedException();
        }

        /// <summary />
        public static TReturn Min<T, TType, TReturn>(this IDatastoreGrouping<TType, T> source, Func<T, TReturn> selector)
        {
            throw new NotImplementedException();
        }

        /// <summary />
        public static int Count<T, TType>(this IDatastoreGrouping<TType, T> source)
        {
            throw new NotImplementedException();
        }

        //public static IDatastoreGroupable<TSourceType> OrderBy<TSourceType, TKey>(this IDatastoreGroupable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector)
        //{
        //    throw new NotImplementedException();
        //}

        #endregion

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

    }
}