using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    internal static class Utilities
    {
        /// <summary />
        private const string ValidDbFieldChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_!@#$%^&*()-,.<> /{}+=? ";
        /// <summary />
        private const string ValidVariableChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_";
        //TODO: ensure does not get too big
        /// <summary />
        private static ConcurrentDictionary<string, string> _tokenCache = new ConcurrentDictionary<string, string>();
        /// <summary />
        private static ConcurrentDictionary<string, string> _tokenCodeCache = new ConcurrentDictionary<string, string>();

        /// <summary />
        public static string DbTokenize(string str)
        {
            try
            {
                if (string.IsNullOrEmpty(str))
                    return string.Empty;

                if (!_tokenCache.ContainsKey(str))
                {
                    var retval = new StringBuilder();
                    foreach (var c in str)
                    {
                        if (!ValidDbFieldChars.Contains(c))
                            retval.Append("_");
                        else
                            retval.Append(c);
                    }
                    _tokenCache.AddOrUpdate(str, retval.ToString(), (key, value) => value);
                }
                return _tokenCache[str];
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary />
        public static string CodeTokenize(string str)
        {
            try
            {
                if (string.IsNullOrEmpty(str))
                    return string.Empty;

                if (!_tokenCodeCache.ContainsKey(str))
                {
                    var retval = new StringBuilder();
                    foreach (var c in str)
                    {
                        if (!ValidVariableChars.Contains(c))
                            retval.Append(((int)c).ToString("X2"));
                        else
                            retval.Append(c);
                    }
                    _tokenCodeCache.AddOrUpdate(str, retval.ToString(), (key, value) => value);
                }
                return _tokenCodeCache[str];
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary />
        public static int CurrentTimestamp
        {
            get { return GetTimestamp(DateTime.UtcNow); }
        }

        /// <summary />
        public static int GetTimestamp(DateTime utcTime)
        {
            return (int)(utcTime - (new DateTime(2000, 1, 1))).TotalSeconds;
        }

        /// <summary />
        public static string LocalIPAddress()
        {
            try
            {
                if (!System.Net.NetworkInformation.NetworkInterface.GetIsNetworkAvailable())
                    return null;

                var host = System.Net.Dns.GetHostEntry(System.Net.Dns.GetHostName());
                return string.Join(",", host
                    .AddressList
                    .Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                    .Select(x => x.ToString()));
            }
            catch { return null; }
        }

        /// <summary />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T Clone<T>(T source)
        {
            if (!typeof(T).IsSerializable)
            {
                throw new ArgumentException("The type must be serializable.", "source");
            }

            // Don't serialize a null object, simply return the default for that object
            if (Object.ReferenceEquals(source, null))
            {
                return default(T);
            }

            IFormatter formatter = new BinaryFormatter();
            var arr = _cloneCache.GetOrAdd(source.GetHashCode(), key =>
            {
                using (var stream = new MemoryStream())
                {
                    formatter.Serialize(stream, source);
                    stream.Seek(0, SeekOrigin.Begin);
                    return stream.ToArray();
                }
            });

            using (var stream = new MemoryStream(arr))
            {
                var v = (T)formatter.Deserialize(stream);
                return v;
            }

        }

        //Timeout in 20 minutes
        private static Cache<int, byte[]> _cloneCache = new Cache<int, byte[]>(new TimeSpan(0, 10, 0), 4391);

    }
}
