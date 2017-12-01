using Gravitybox.Datastore.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.QueryBuilders
{
    internal static class ListDimensionCache
    {
        //Each repository will have its own ConcurrentDictionary
        //On each call the ConcurrentDictionary for the repository will be referenced or added
        //A Add or Get will be performed on the repository ConcurrentDictionary based on the dimension queried

        private static ConcurrentDictionary<int, ConcurrentDictionary<string, LDCacheItem>> _cache { get; } = new ConcurrentDictionary<int, ConcurrentDictionary<string, LDCacheItem>>();
#if DEBUG
        private const int TimeCheck = 10; //seconds
        private const int MaxCacheTime = 1; //minutes
#else
        private const int TimeCheck = 120; //seconds
        private const int MaxCacheTime = 5; //minutes
#endif
        private static System.Timers.Timer _timer = null;

        static ListDimensionCache()
        {
            _timer = new System.Timers.Timer(TimeCheck * 1000);
            _timer.Elapsed += _timer_Elapsed;
            _timer.Start();
        }

        private static void _timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            _timer.Stop();
            var timer = Stopwatch.StartNew();
            var itemCount = 0;
            var removed = 0;
            var hitCount = 0;
            try
            {
                foreach (var repositoryId in _cache.Keys)
                {
                    lock (_cache)
                    {
                        var rLookup = _cache.GetOrAdd(repositoryId, key => new ConcurrentDictionary<string, LDCacheItem>());
                        foreach (var key in rLookup.Keys)
                        {
                            var cacheContainer = rLookup[key];
                            if (DateTime.Now.Subtract(cacheContainer.Timestamp).TotalMinutes >= MaxCacheTime)
                            {
                                removed += cacheContainer.Cache.Count;
                                LDCacheItem v;
                                var b = rLookup.TryRemove(key, out v);
                            }
                            else
                            {
                                itemCount += cacheContainer.Cache.Count;
                                hitCount += cacheContainer.HitCount;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                LoggerCQ.LogWarning("ListDimensionCache housekeeping failed");
            }
            finally
            {
                timer.Stop();
                //LoggerCQ.LogDebug($"ListDimensionCache: RepositoryCount={_cache.Keys.Count}, ItemCount={itemCount}, HitCount={hitCount}, Elapsed={timer.ElapsedMilliseconds}, RemovedItems={removed}");
                _timer.Start();
            }
        }

        public static void Add(int repositoryId, long didx, DataQuery query, Dictionary<long, IRefinementItem> data)
        {
            var cacheKey = didx + "|" + query.CoreWhereHashCode();
            var rLookup = _cache.GetOrAdd(repositoryId, key => new ConcurrentDictionary<string, LDCacheItem>());
            var b = rLookup.TryAdd(cacheKey, new LDCacheItem { Cache = data });

            //LoggerCQ.LogInfo("ListDimensionCache.Add: RepositoryId=" + repositoryId + ", Count=" + rLookup.Count);
        }

        public static Dictionary<long, IRefinementItem> Get(int repositoryId, long didx, DataQuery query)
        {
            //If the dimensions are not needed then assume cached already since all counts will be zero anyway
            if (!query.IncludeDimensions)
                return new Dictionary<long, IRefinementItem>();

            var cacheKey = didx + "|" + query.CoreWhereHashCode();
            lock (_cache)
            {
                var rLookup = _cache.GetOrAdd(repositoryId, key => new ConcurrentDictionary<string, LDCacheItem>());
                LDCacheItem retval = null;
                var b = rLookup.TryGetValue(cacheKey, out retval);

                //LoggerCQ.LogInfo("ListDimensionCache.Get: RepositoryId=" + repositoryId + ", Count=" + (retval == null ? "-1" : retval.Count.ToString()));
                if (retval != null)
                {
                    retval.Timestamp = DateTime.Now;
                    retval.HitCount++;
                }
                return retval?.Cache;
            }
        }

        public static void Clear(int repositoryId)
        {
            ConcurrentDictionary<string, LDCacheItem> v = null;
            var b = _cache.TryRemove(repositoryId, out v);
        }

        private class LDCacheItem
        {
            public Dictionary<long, IRefinementItem> Cache { get; set; }
            public DateTime Timestamp { get; set; } = DateTime.Now;
            public int HitCount { get; set; } = 0;
        }
    }
}