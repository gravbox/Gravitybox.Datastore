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
using System.Runtime.CompilerServices;
using Gravitybox.Datastore.Server.Core.QueryBuilders;

namespace Gravitybox.Datastore.Server.Core
{
    /// <summary>
    /// Each repository has its own cache. The Manager will get/create the cache list
    /// </summary>
    internal class QueryCache
    {
        public FTSReadyCache FTSReadyCache { get; private set; } = new FTSReadyCache();

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

            //Clear everything and restart clean
            internal static void Reset()
            {
                lock (_locker)
                {
                    _lockCache.Clear();
                }
            }
        }
        #endregion

        #region Members
        private int _maxItems = 0; //Number of cache items to keep
        private List<CacheResultsSlice> _cacheSlice = new List<CacheResultsSlice>();
        private Dictionary<int, FieldDefinition> _schemaDatagrouping = new Dictionary<int, FieldDefinition>();
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
                _timer.Elapsed += TimerElapsed;
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
        private void TimerElapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            var lockTime = 0;
            var cacheCount = 0;
            _timer.Stop();
            try
            {
                var timer = Stopwatch.StartNew();
                var count = 0;

                var allCaches = RepositoryCacheManager.All;
                cacheCount = allCaches.Count;
                foreach (var cache in allCaches)
                {
                    using (var q = new AcquireWriterLock(ServerUtilities.RandomizeGuid(cache.ID, RSeed), "QueryCache"))
                    {
                        lockTime += q.LockTime;
                        _maxItems = System.Math.Max(0, ConfigHelper.QueryCacheCount);

                        //Purge anything not used in the last N minutes
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

                //Purge anything not used in the last N minutes
                count += _cacheSlice.RemoveAll(x => DateTime.Now.Subtract(x.Timestamp).TotalMinutes >= CacheExpireMinutes);
                #endregion

                timer.Stop();

                //Log it if too long
                if (timer.ElapsedMilliseconds > 2000)
                    LoggerCQ.LogWarning($"QueryCache housekeeping: Elapsed={timer.ElapsedMilliseconds}, LockTime={lockTime}, ItemsRemoved={count}, CacheCount={cacheCount}");
            }
            catch (Exception ex)
            {
                LoggerCQ.LogWarning(ex, "QueryCache housekeeping failed");
            }
            finally
            {
                _timer.Start();
            }
        }
        #endregion

        #region Methods
        public DataQueryResults Get(DatastoreEntities context, RepositorySchema schema, DataQuery query, int repositoryId, Guid id, out bool isCore)
        {
            isCore = false;
            if (!ConfigHelper.AllowCaching)
                return null;

            long lockTime = 0;
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

                    CacheResultsQuery item = null;
                    lock (cache)
                    {
                        item = cache?.FirstOrDefault(x => x.QueryHash == queryHash && x.ChangeStamp == changeStamp);
                    }

                    if (item == null) //return null;
                    {
                        if (ConfigHelper.AllowCoreCache)
                        {
                            //TODO: OPTIMIZE: this is a linear search of thousands of items!!!!
                            //If did not find a match then find see if core properties match
                            //If so we can use the dimension and count values and just replace the records collection
                            lock (cache)
                            {
                                item = cache?.FirstOrDefault(x => x.QueryCoreHash == coreHash && x.ChangeStamp == changeStamp);
                            }
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
                LoggerCQ.LogError(ex, $"RepositoryId={id}, Elapsed={timer.ElapsedMilliseconds}, LockTime={lockTime}, Count={cache.Count}, QueryHash={queryHash}, ChangeStamp={changeStamp}, ID={id}");
                throw;
            }
            finally
            {
                timer.Stop();
                if (timer.ElapsedMilliseconds > 50)
                    LoggerCQ.LogWarning($"Slow cache get: Elapsed={timer.ElapsedMilliseconds}, LockTime={lockTime}, Count={cache.Count}, ID={id}, QueryString=\"{query.ToString()}\"");
            }
        }

        public void Set(DatastoreEntities context, RepositorySchema schema, DataQuery query, int repositoryId, Guid id, DataQueryResults results)
        {
            if (!ConfigHelper.AllowCaching) return;
            if (results == null) return;

            //Do not cache big items
            if (results.RecordList.Count > 100) return;
            if (!string.IsNullOrEmpty(query.Keyword) && !this.FTSReadyCache.IsReady(id)) return;
            //if (!string.IsNullOrEmpty(query.Keyword) && !ConfigHelper.AllowCacheWithKeyword) return;

            var timer = Stopwatch.StartNew();
            var cache = RepositoryCacheManager.GetCache(id, RepositoryManager.GetSchemaParentId(repositoryId));
            long lockTime = 0;
            var changeStamp = 0;
            var queryHash = 0;
            var subCacheKey = GetSubKey(schema, query);
            try
            {
                //Some queries should be cached a long time
                var longCache = !query.FieldFilters.Any() &&
                    !query.FieldSorts.Any() &&
                    string.IsNullOrEmpty(query.Keyword) &&
                    !query.SkipDimensions.Any();
                var extraMinutes = longCache ? 480 : 0;

                var coreHash = 0;
                CacheResultsQuery item;

                using (var q = new AcquireReaderLock(ServerUtilities.RandomizeGuid(cache.ID, RSeed), "QueryCache"))
                {
                    lockTime += q.LockTime;
                    queryHash = query.GetHashCode();
                    if (!query.ExcludeCount && query.IncludeDimensions && !query.IncludeEmptyDimensions)
                        coreHash = query.CoreHashCode();

                    changeStamp = RepositoryManager.GetRepositoryChangeStamp(context, repositoryId);
                    lock (cache)
                    {
                        item = cache?.FirstOrDefault(x => x.QueryHash == queryHash && x.ChangeStamp == changeStamp);
                    }

                    //If data has not changed and results are in cache then do nothing except mark as accessed
                    if (item != null)
                    {
                        item.Results = results;
                        item.Timestamp = DateTime.Now.AddMinutes(extraMinutes);
                        item.SubKey = subCacheKey;
                        return;
                    }
                }

                lock (cache)
                {
                    using (var q = new AcquireWriterLock(ServerUtilities.RandomizeGuid(cache.ID, RSeed), "QueryCache"))
                    {
                        lockTime += q.LockTime;

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
                            SubKey = subCacheKey,
                        };
                        cache.Add(item);
                    }
                }
            }
            catch (Exception ex)
            {
                timer.Stop();
                LoggerCQ.LogError(ex, $"RepositoryId={id}, Elapsed={timer.ElapsedMilliseconds}, ID={id}, LockTime={lockTime}, Count={cache.Count}, QueryHash={queryHash}, ChangeStamp={changeStamp}");
                throw;
            }
            finally
            {
                timer.Stop();
                if (timer.ElapsedMilliseconds > 50)
                    LoggerCQ.LogWarning($"Slow cache set: Elapsed={timer.ElapsedMilliseconds}, LockTime={lockTime}, Count={cache.Count}, ID={id}, Query=\"{query.ToString()}\"");
                LoggerCQ.LogTrace($"QueryCache: Set: SubCacheKey={subCacheKey}");
            }
        }

        /// <summary>
        /// Find the cache subkey. This only exists if there is a FieldFilter with the "GroupingField=Value"
        /// </summary>
        private string GetSubKey(RepositorySchema schema, DataQuery query)
        {
            FieldDefinition groupingField = null;

            //Get the data grouping field for this schema and cache
            lock (_schemaDatagrouping)
            {
                if (schema.ParentID == null && !_schemaDatagrouping.TryGetValue(schema.InternalID, out groupingField))
                {
                    groupingField = schema.FieldList.FirstOrDefault(x => x.IsDataGrouping);
                    if (groupingField == null) groupingField = new FieldDefinition();
                    _schemaDatagrouping.Add(schema.InternalID, groupingField);
                }
            }

            //If there is a grouping field then use it to narrow search
            if (groupingField != null)
            {
                var ff = query.FieldFilters.FirstOrDefault(x => x.Name.Match(groupingField.Name));
                if (ff != null && ff.Comparer == ComparisonConstants.Equals && ff.Value != null)
                {
                    return ff.Value.ToString().ToLower();
                }
            }

            return null;
        }

        public SummarySliceValue GetSlice(DatastoreEntities context, SummarySlice slice, int repositoryId)
        {
            if (!ConfigHelper.AllowCaching)
                return null;

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

        public void SetSlice(DatastoreEntities context, SummarySlice slice, int repositoryId, SummarySliceValue results)
        {
            if (!ConfigHelper.AllowCaching)
                return;

            int changeStamp = 0;
            int queryHash = 0;
            CacheResultsSlice item;
            using (var q = new AcquireReaderLock(QueryCacheID, "QueryCache"))
            {
                //Do not cache big items
                if (results.RecordList.Count > 500)
                    return;
                if (slice.Query != null && !string.IsNullOrEmpty(slice.Query.Keyword) && !ConfigHelper.AllowCacheWithKeyword)
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

        /// <summary>
        /// Invalidate the cache for a specific Repository
        /// </summary>
        public void Clear(int repositoryId, Guid id, string reason, string cacheSubKey = null)
        {
            try
            {
                var count = 0;
                var cache = RepositoryCacheManager.GetCache(id, RepositoryManager.GetSchemaParentId(repositoryId));
                this.FTSReadyCache.Clear(id);
                ListDimensionCache.Clear(repositoryId);

                using (var q = new AcquireWriterLock(ServerUtilities.RandomizeGuid(cache.ID, RSeed), "QueryCache"))
                {
                    if (cacheSubKey == null)
                    {
                        count += cache.Count;
                        cache.Clear(); //Clear entire cache
                        LoggerCQ.LogTrace($"QueryCache: Clear Full, ID={id}, Count={count}");
                    }
                    else
                    {
                        //Clear all based on subkey AND with no key since it is unknown what data is in those
                        count += cache.RemoveAll(x => x.SubKey == cacheSubKey.ToLower() || x.SubKey == null);
                        LoggerCQ.LogTrace($"QueryCache: SubKey={cacheSubKey}, ID={id}, Count={count}");
                    }
                        
                    if (_schemaDatagrouping.ContainsKey(repositoryId))
                        _schemaDatagrouping.Remove(repositoryId);
                }

                //Find caches where this is the parent and clear them all too
                var parentCaches = RepositoryCacheManager.All.Where(x => x.ParentId == repositoryId);
                foreach (var pcache in parentCaches)
                {
                    using (var q = new AcquireWriterLock(ServerUtilities.RandomizeGuid(pcache.ID, RSeed), "QueryCache"))
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
                ListDimensionCache.Clear(repositoryId);

                //Log the invalidation
                Task.Factory.StartNew(() =>
                {
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        var newItem = new EFDAL.Entity.CacheInvalidate { Count = count, RepositoryId = repositoryId };
                        newItem.SetValue(EFDAL.Entity.CacheInvalidate.FieldNameConstants.Reason, reason, true);
                        newItem.SetValue(EFDAL.Entity.CacheInvalidate.FieldNameConstants.Subkey, cacheSubKey, true);
                        context.AddItem(newItem);
                        context.SaveChanges();
                    }
                });
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
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

        /// <summary>
        /// Clear everything and restart clean
        /// </summary>
        public void Reset()
        {
            RepositoryCacheManager.Reset();
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
            /// <summary>
            /// This is used to further allow for sub-invalidation instead of entire list
            /// </summary>
            public string SubKey { get; set; }
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