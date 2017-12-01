#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;
using Gravitybox.Datastore.Server.Interfaces;
using System.Runtime.CompilerServices;

namespace Gravitybox.Datastore.Server.Core
{
    /// <summary>
    /// Each repository has its own cache. The Manager will get/create the cache list
    /// </summary>
    internal class QueryCache
    {
        #region RepositoryCache
        private class RepositoryCache : List<CacheResultsQuery>
        {
            public RepositoryCache(Guid id, int? parentId)
            {
                this.ID = id;
                this.ParentId = ParentId;
            }

            public Guid ID { get; private set; }

            public int? ParentId { get; private set; }
        }
        #endregion

        #region RepositoryCacheManager
        private static class RepositoryCacheManager
        {
            private static Dictionary<Guid, RepositoryCache> _lockCache = new Dictionary<Guid, RepositoryCache>();
            private static object _locker = new object();

            internal static RepositoryCache GetCache(Guid id, int? parentId)
            {
                lock (_locker)
                {
                    if (!_lockCache.ContainsKey(id))
                        _lockCache.Add(id, new RepositoryCache(id, parentId));
                    return _lockCache[id];
                }
            }

            internal static List<RepositoryCache> All
            {
                get
                {
                    lock (_locker)
                    {
                        return _lockCache.Values.ToList();
                    }
                }
            }
        }
        #endregion

        #region Members
        private int _maxItems = 0; //Number of cache items to keep
        private List<CacheResultsSlice> _cacheSlice = new List<CacheResultsSlice>();
        private System.Timers.Timer _timer = null;
        private readonly Guid QueryCacheID = new Guid("19A552CA-C94A-4FC5-8DD2-A897F0035BEE");
        private const int CacheExpireMinutes = 20;
        private const int TIMECHECK = 60000;
        private const int RSeed = 89;
        #endregion

        #region Constructor
        public QueryCache()
        {
            try
            {
                _maxItems = ConfigHelper.QueryCacheCount;

                //Cull cache every minute
                _timer = new System.Timers.Timer(TIMECHECK);
                _timer.Elapsed += _timer_Elapsed;
                _timer.Start();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                //throw;
            }
        }
        #endregion

        #region Timer
        private void _timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            _timer.Stop();
            try
            {
                var timer = Stopwatch.StartNew();
                var count = 0;

                var allCaches = RepositoryCacheManager.All;
                foreach (var cache in allCaches)
                {
                    using (var q = new AcquireWriterLock(ServerUtilities.RandomizeGuid(cache.ID, RSeed), "QueryCache"))
                    {
                        _maxItems = System.Math.Max(0, ConfigHelper.QueryCacheCount);

                        //Purge anything not used in the last X minutes
                        count += cache.RemoveAll(x => DateTime.Now.Subtract(x.Timestamp).TotalMinutes >= CacheExpireMinutes);

                        //Keep only the last N items
                        cache.OrderByDescending(x => x.Timestamp)
                            .Skip(_maxItems)
                            .ToList()
                            .ForEach(x => { cache.Remove(x); count++; });
                    }
                }

                #region Now do the Slices
                //Keep only the last N items
                _cacheSlice.OrderByDescending(x => x.Timestamp)
                        .Skip(_maxItems)
                        .ToList()
                        .ForEach(x => { _cacheSlice.Remove(x); count++; });

                //Purge anything not used in the last X minutes
                count += _cacheSlice.RemoveAll(x => DateTime.Now.Subtract(x.Timestamp).TotalMinutes >= CacheExpireMinutes);
                #endregion

                timer.Stop();

                //Log it if too long
                if (timer.ElapsedMilliseconds > 2000)
                    LoggerCQ.LogWarning("QueryCache housekeeping: Elapsed=" + timer.ElapsedMilliseconds + ", Removed=" + count);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogWarning("QueryCache housekeeping failed");
            }
            finally
            {
                _timer.Start();
            }
        }
        #endregion

        #region Methods
        public DataQueryResults Get(DatastoreEntities context, DataQuery query, int repositoryId, Guid id, out bool isCore)
        {
            isCore = false;
            if (!ConfigHelper.AllowCaching)
                return null;

            var lockTime = 0;
            int queryHash = 0;
            int coreHash = 0;
            int changeStamp = 0;

            var task1 = Task.Factory.StartNew(() =>
            {
                queryHash = query.GetHashCode();
                coreHash = query.CoreHashCode();
                changeStamp = RepositoryManager.GetRepositoryChangeStamp(context, repositoryId);
            });

            var timer = new Stopwatch();
            var cache = RepositoryCacheManager.GetCache(id, RepositoryManager.GetSchemaParentId(repositoryId));
            try
            {
                using (var q = new AcquireReaderLock(ServerUtilities.RandomizeGuid(cache.ID, RSeed), "QueryCache"))
                {
                    lockTime = q.LockTime;
                    timer.Start();

                    //Ensure that the pre-calculations are complete
                    task1.Wait();

                    var item = cache.FirstOrDefault(x => x.QueryHash == queryHash && x.ChangeStamp == changeStamp);
                    if (item == null) //return null;
                    {
                        if (ConfigHelper.AllowCoreCache)
                        {
                            //TODO: OPTIMIZE: this is a linear search of thousands of items!!!!
                            //If did not find a match then find see if core properties match
                            //If so we can use the dimension and count values and just replace the records collection
                            item = cache.FirstOrDefault(x => x.QueryCoreHash == coreHash && x.ChangeStamp == changeStamp);
                        }
                        if (item == null) return null;
                        isCore = true;
                        item.HitCount++;
                        return item.Results;
                    }
                    item.Timestamp = DateTime.Now;
                    item.HitCount++;
                    return item.Results;
                }
            }
            catch (Exception ex)
            {
                timer.Stop();
                LoggerCQ.LogError(ex, $"RepositoryId={id}, Elapsed={timer.ElapsedMilliseconds}, LockTime={lockTime}, Count={cache.Count}, ID={id}");
                throw;
            }
            finally
            {
                timer.Stop();
                if (timer.ElapsedMilliseconds > 50)
                    LoggerCQ.LogWarning($"Slow cache get: Elapsed={timer.ElapsedMilliseconds}, LockTime={lockTime}, Count={cache.Count}, ID={id}, QueryString =\"{query.ToString()}\"");
            }
        }

        public void Set(DatastoreEntities context, DataQuery query, int repositoryId, Guid id, DataQueryResults results)
        {
            if (!ConfigHelper.AllowCaching) return;
            if (results == null) return;

            //Do not cache big items
            if (results.RecordList.Count > 100) return;
            if (!string.IsNullOrEmpty(query.Keyword)) return;

            var timer = Stopwatch.StartNew();
            var cache = RepositoryCacheManager.GetCache(id, RepositoryManager.GetSchemaParentId(repositoryId));
            try
            {
                //Some queries should be cached a long time
                var longCache = !query.FieldFilters.Any() &&
                    !query.FieldSorts.Any() &&
                    string.IsNullOrEmpty(query.Keyword) &&
                    !query.SkipDimensions.Any();
                var extraMinutes = longCache ? 480 : 0;

                var changeStamp = 0;
                var queryHash = 0;
                var coreHash = 0;
                CacheResultsQuery item;
                using (var q = new AcquireReaderLock(ServerUtilities.RandomizeGuid(cache.ID, RSeed), "QueryCache"))
                {
                    queryHash = query.GetHashCode();
                    if (!query.ExcludeCount && query.IncludeDimensions && !query.IncludeEmptyDimensions)
                        coreHash = query.CoreHashCode();

                    changeStamp = RepositoryManager.GetRepositoryChangeStamp(context, repositoryId);
                    item = cache.FirstOrDefault(x => x.QueryHash == queryHash && x.ChangeStamp == changeStamp);
                    //If data has not changed and results are in cache then do nothing except mark as accessed
                    if (item != null)
                    {
                        item.Results = results;
                        item.Timestamp = DateTime.Now.AddMinutes(extraMinutes);
                        return;
                    }
                }

                using (var q = new AcquireWriterLock(ServerUtilities.RandomizeGuid(cache.ID, RSeed), "QueryCache"))
                {
                    //Remove previous cache
                    cache.RemoveAll(x => x.QueryHash == queryHash);

                    //Create a new cache item
                    item = new CacheResultsQuery()
                    {
                        QueryHash = queryHash,
                        QueryCoreHash = coreHash,
                        RepositoryId = repositoryId,
                        ChangeStamp = changeStamp,
                        Results = results,
                        QueryString = query.ToString(),
                        ParentId = RepositoryManager.GetSchemaParentId(repositoryId),
                        Timestamp = DateTime.Now.AddMinutes(extraMinutes),
                    };
                    cache.Add(item);
                }
            }
            catch (Exception ex)
            {
                timer.Stop();
                LoggerCQ.LogError(ex, $"RepositoryId={id}, Elapsed={timer.ElapsedMilliseconds}, ID={id}, Count={cache.Count}");
                throw;
            }
            finally
            {
                timer.Stop();
                if (timer.ElapsedMilliseconds > 50)
                    LoggerCQ.LogWarning($"Slow cache set: Elapsed={timer.ElapsedMilliseconds}, ID={id}, Count={cache.Count}");
            }
        }

        public SummarySliceValue GetSlice(DatastoreEntities context, SummarySlice slice, int repositoryId)
        {
            if (!ConfigHelper.AllowCaching)
                return null;

            try
            {
                using (var q = new AcquireReaderLock(QueryCacheID, "QueryCache"))
                {
                    var queryHash = slice.GetHashCode();
                    var changeStamp = RepositoryManager.GetRepositoryChangeStamp(context, repositoryId);
                    var item = _cacheSlice.FirstOrDefault(x => x.QueryHash == queryHash && x.RepositoryId == repositoryId && x.ChangeStamp == changeStamp);
                    if (item == null) return null;
                    item.Timestamp = DateTime.Now;
                    item.HitCount++;
                    return item.Results;
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public void SetSlice(DatastoreEntities context, SummarySlice slice, int repositoryId, SummarySliceValue results)
        {
            if (!ConfigHelper.AllowCaching)
                return;

            try
            {
                int changeStamp = 0;
                int queryHash = 0;
                CacheResultsSlice item;
                using (var q = new AcquireReaderLock(QueryCacheID, "QueryCache"))
                {
                    //Do not cache big items
                    if (results.RecordList.Count > 500)
                        return;
                    if (slice.Query != null && !string.IsNullOrEmpty(slice.Query.Keyword))
                        return;

                    queryHash = slice.GetHashCode();
                    changeStamp = RepositoryManager.GetRepositoryChangeStamp(context, repositoryId);
                    item = _cacheSlice.FirstOrDefault(x => x.QueryHash == queryHash &&
                                                      x.RepositoryId == repositoryId &&
                                                      x.ChangeStamp == changeStamp);

                    //If data has not changed and results are in cache then do nothing except mark as accessed
                    if (item != null)
                    {
                        item.Results = results;
                        item.Timestamp = DateTime.Now;
                        return;
                    }
                }

                using (var q = new AcquireWriterLock(QueryCacheID, "QueryCache"))
                {
                    //Remove previous cache
                    _cacheSlice.RemoveAll(x => x.QueryHash == queryHash && x.RepositoryId == repositoryId);

                    //Create a new cache item
                    item = new CacheResultsSlice()
                    {
                        QueryHash = queryHash,
                        RepositoryId = repositoryId,
                        ChangeStamp = changeStamp,
                        Results = results,
                        QueryString = slice.ToString(),
                        ParentId = RepositoryManager.GetSchemaParentId(repositoryId),
                    };
                    _cacheSlice.Add(item);
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        /// <summary>
        /// Invalidate the cache for a specific Repository
        /// </summary>
        public void Clear(int repositoryId, Guid id)
        {
            try
            {
                var count = 0;
                var cache = RepositoryCacheManager.GetCache(id, RepositoryManager.GetSchemaParentId(repositoryId));
                using (var q = new AcquireReaderLock(ServerUtilities.RandomizeGuid(cache.ID, RSeed), "QueryCache"))
                {
                    count += cache.Count;
                    cache.Clear();
                }

                //Find caches where this is the parent and clear them all too
                var parentCaches = RepositoryCacheManager.All.Where(x => x.ParentId == repositoryId);
                foreach (var pcache in parentCaches)
                {
                    using (var q = new AcquireReaderLock(ServerUtilities.RandomizeGuid(pcache.ID, RSeed), "QueryCache"))
                    {
                        count += pcache.Count;
                        pcache.Clear();
                    }
                }

                using (var q = new AcquireWriterLock(QueryCacheID, "QueryCache"))
                {
                    count += _cacheSlice.RemoveAll(x => x.RepositoryId == repositoryId);
                    count += _cacheSlice.RemoveAll(x => x.ParentId == repositoryId);
                }

                //If the query cache is being cleared then the List dimension count cache should be too
                QueryBuilders.ListDimensionCache.Clear(repositoryId);

                //Log the invalidation
                Task.Factory.StartNew(() =>
                {
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        context.AddItem(new EFDAL.Entity.CacheInvalidate { Count = count, RepositoryId = repositoryId });
                        context.SaveChanges();
                    }
                });

            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        /// <summary>
        /// This is a count of all items in cache
        /// </summary>
        public int Count
        {
            get
            {
                var l = RepositoryCacheManager.All;
                if (l.Count == 0) return 0;
                return l.Sum(x => x.Count);
            }
        }
        #endregion

        #region CacheResultsBase
        private abstract class CacheResultsBase
        {
            public CacheResultsBase()
            {
                this.Timestamp = DateTime.Now;
            }

            public override string ToString()
            {
                return $"RepositoryId={this.RepositoryId}, ChangeStamp={this.ChangeStamp}, QueryHash={this.QueryHash}, HitCount={this.HitCount}, Timestamp=" + this.Timestamp.ToString("HH:mm:ss.tt");
            }

            public int RepositoryId { get; set; }
            public int? ParentId { get; set; }
            public DateTime Timestamp { get; set; }
            public long HitCount { get; set; } = 1;
            public int ChangeStamp { get; set; }
            public int QueryHash { get; set; }
            public int QueryCoreHash { get; set; }
            public string QueryString { get; set; }
            protected DateTime _lastUsed = DateTime.Now;
        }
        #endregion

        #region CacheResultsQuery
        /// <summary>
        /// This is a cache object of exactly one query on a repository as uniquely defined by the query string
        /// </summary>
        private class CacheResultsQuery : CacheResultsBase
        {
            public CacheResultsQuery()
                : base()
            {
            }

            private DataQueryResults _next = null;

            public DataQueryResults Results
            {
                get
                {
                    this._lastUsed = DateTime.Now;
                    return ObjectCloner<DataQueryResults>.Clone(this._next);
                }
                set
                {
                    this._lastUsed = DateTime.Now;
                    this._next = ObjectCloner<DataQueryResults>.Clone(value);
                }
            }

            public int Size
            {
                get { return 0; }
            }

            ~CacheResultsQuery()
            {
                this._next = null;
            }
        }
        #endregion

        #region CacheResultsSlice
        private class CacheResultsSlice : CacheResultsBase
        {
            private SummarySliceValue _next = null;

            public CacheResultsSlice()
                : base()
            {
            }

            public SummarySliceValue Results
            {
                get
                {
                    this._lastUsed = DateTime.Now;
                    return ObjectCloner<SummarySliceValue>.Clone(this._next);
                }
                set
                {
                    this._lastUsed = DateTime.Now;
                    this._next = ObjectCloner<SummarySliceValue>.Clone(value);
                }
            }

            ~CacheResultsSlice()
            {
                this._next = null;
            }

        }
        #endregion

    }
}