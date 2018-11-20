#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using Gravitybox.Datastore.Common;
using System.IO;
using System.Threading.Tasks;
using System.Diagnostics;
using Gravitybox.Datastore.EFDAL;
using System.Collections.Concurrent;
using Gravitybox.Datastore.Server.Core.Housekeeping;
using Gravitybox.Datastore.Common.Queryable;
using Gravitybox.Datastore.Common.Exceptions;
using System.Threading;

namespace Gravitybox.Datastore.Server.Core
{
    [Serializable]
    [ServiceBehavior(InstanceContextMode = InstanceContextMode.Single, ConcurrencyMode = ConcurrencyMode.Multiple)]
    public class RepositoryManager : Gravitybox.Datastore.Common.IDataModel
    {
        public const string TraceInfoUpdateData = "UpdateData";
        private static ISystemCore _system = null;
        private static List<string> _fileGroups = new List<string>();
        private static Random _rnd = new Random();
        private DimensionCache _dimensionCache = new DimensionCache();
        private static Dictionary<int, int> _dimensionChangeStampCache = new Dictionary<int, int>();
        private static Dictionary<Guid, int> _repositoryChangeStampCache = new Dictionary<Guid, int>();
        private static readonly SchemaCache _schemaCache = new SchemaCache();
        private static ConcurrentHashSet<Guid> _repositoryExistCache = new ConcurrentHashSet<Guid>();
        private readonly QueryLogManager _queryLogManager = new QueryLogManager();
        private static readonly Guid DimensionCacheID = new Guid("2726FFC1-F4F6-477E-B9AC-B10E59C4BD63");
        private static readonly Guid RepositoryChangeStampID = new Guid("9941FFC1-F4F6-477E-B9AC-B10E59C4BD63");
        private static TableStatsMaintenace _statsMaintenance = null;
        private static HousekeepingMonitor _housekeepingMonitor = new HousekeepingMonitor();
        private bool _masterReset = false;
        private const byte RSeedPermissions = 187;

        //every start is a new instance. this is used to keep track of failover
        internal static Guid InstanceId { get; private set; } = Guid.NewGuid();

        internal static QueryCache QueryCache { get; private set; } = new QueryCache();

        public RepositoryManager(ISystemCore system)
        {
            //DatastoreLock.RegisterMachine();
            _system = system;
            _fileGroups = SqlHelper.GetFileGroups(ConfigHelper.ConnectionString);
            LoggerCQ.LogInfo($"Filegroups: Count={_fileGroups.Count}");

            _statsMaintenance = new TableStatsMaintenace(_system.EnableHouseKeeping);

            //This will process the Async update where statements
            //_timerUpdateDataWhereAsync = new System.Timers.Timer(10000);
            _timerUpdateDataWhereAsync = new System.Timers.Timer(2000);
            _timerUpdateDataWhereAsync.Elapsed += _timerUpdateDataWhereAsync_Elapsed;
            _timerUpdateDataWhereAsync.Start();

            #region Reset thread pool to larger count
            try
            {
                int a; int b;
                ThreadPool.SetMinThreads(200, 200);
                ThreadPool.GetMinThreads(out a, out b);
                LoggerCQ.LogInfo($"Thread Pool Min Count={b}, WorkerThreads={a}");
                ThreadPool.GetMaxThreads(out a, out b);
                ThreadPool.SetMaxThreads(a, 10000);
                ThreadPool.GetMaxThreads(out a, out b);
                LoggerCQ.LogInfo($"Thread Pool Completion Port Count={b}, WorkerThreads={a}");
            }
            catch
            {
                LoggerCQ.LogWarning("Failed to initialize thread count");
            }
            #endregion

            Task.Factory.StartNew(() => { _schemaCache.Populate(); });
        }

        public static ISystemCore SystemCore
        {
            get { return _system; }
        }

        public static void SetRepositoryChangeStamp(Guid id)
        {
            if (!IsMaster())
                throw new NotMasterInstanceException();

            try
            {
                using (var l4 = new AcquireWriterLock(RepositoryChangeStampID, "RepositoryChangeStamp"))
                {
                    if (!_repositoryChangeStampCache.ContainsKey(id))
                        _repositoryChangeStampCache.Add(id, 0);
                    _repositoryChangeStampCache[id] = SqlHelper.GetChangeStamp();
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw new FaultException(ex.Message);
            }
        }

        public static int GetRepositoryChangeStamp(DatastoreEntities context, int id)
        {
            if (!IsMaster())
                throw new NotMasterInstanceException();

            try
            {
                using (var q = new AcquireWriterLock(DimensionCacheID, "DimensionCache"))
                {
                    if (!_dimensionChangeStampCache.ContainsKey(id))
                        _dimensionChangeStampCache.Add(id, SqlHelper.GetChangeStamp());
                    return _dimensionChangeStampCache[id];
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw new FaultException(ex.Message);
            }
        }

        public static void SetDimensionChanged(int id)
        {
            if (!IsMaster())
                throw new NotMasterInstanceException();

            try
            {
                using (var q = new AcquireWriterLock(DimensionCacheID, "DimensionCache"))
                {
                    if (!_dimensionChangeStampCache.ContainsKey(id))
                        _dimensionChangeStampCache.Add(id, 0);
                    _dimensionChangeStampCache[id] = SqlHelper.GetChangeStamp();
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw new FaultException(ex.Message);
            }
        }

        public static int GetDimensionChanged(DatastoreEntities context, int id)
        {
            if (!IsMaster())
                throw new NotMasterInstanceException();

            try
            {
                using (var q = new AcquireWriterLock(DimensionCacheID, "DimensionCache"))
                {
                    if (!_dimensionChangeStampCache.ContainsKey(id))
                        _dimensionChangeStampCache.Add(id, 0);
                    return _dimensionChangeStampCache[id];
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw new FaultException(ex.Message);
            }
        }

        public void RemoveRepository(Guid repositoryId)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            try
            {
                var lockTime = 0;
                var timer = Stopwatch.StartNew();
                timer.Start();

                //Delete from list in case error
                var id = 0;
                using (var q = new AcquireWriterLock(repositoryId, "RemoveRepository"))
                {
                    lockTime = q.LockTime;
                    if (!RepositoryExists(repositoryId)) return;
                    var schema = GetSchema(repositoryId);
                    if (schema == null) return;
                    id = schema.InternalID;
                    LoggerCQ.LogDebug($"Starting RemoveRepository: ID={repositoryId}, InternalId={id}");

                    //Hold lock long enough to mark as deleted
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        var r = context.Repository.FirstOrDefault(x => x.UniqueKey == repositoryId);
                        if (r != null)
                        {
                            if (context.Repository.Any(x => x.ParentId == r.RepositoryId))
                            {
                                //If there are any repositories that use this as a base then remove them too
                                context.Repository
                                    .Where(x => x.ParentId == r.RepositoryId)
                                    .Select(x => x.UniqueKey)
                                    .ToList()
                                    .ForEach(x => RemoveRepository(x));
                            }
                            context.DeleteItem(r);
                            context.SaveChanges();

                            //Queue the log clearing for later
                            _housekeepingMonitor.QueueTask(new HkClearRepositoryLog { PivotDate = DateTime.Now, RepositoryId = id });
                        }
                    }

                    SqlHelper.RemoveRepository(ConfigHelper.ConnectionString, repositoryId);
                    this.RepositoryCacheRemove(repositoryId);
                    QueryCache.Clear(schema.InternalID, schema.ID);
                    ClearSchemaCache(schema.ID);
                }

                timer.Stop();
                LoggerCQ.LogDebug($"RemoveRepository: ID={repositoryId}, Elapsed={timer.ElapsedMilliseconds}, LockTime={lockTime}, WorkTime={CalcWorkTime(timer, lockTime)}");
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                throw new FaultException(ex.Message);
            }
        }

        public void AddRepository(RepositorySchema schema)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            try
            {
                var timer = Stopwatch.StartNew();
                timer.Start();
                using (var q = new AcquireWriterLock(schema.ID, "AddRepository"))
                {
                    //Ensure that the item is gone
                    RemoveRepository(schema.ID);

                    //Get parent schema for merge
                    long didx;
                    if (schema.ParentID == null)
                        didx = Constants.DGROUP;
                    else
                        didx = Constants.DGROUPEXT;

                    RepositorySchema parentSchema = null;
                    if (schema.ParentID != null)
                    {
                        if (string.IsNullOrEmpty(schema.ObjectAlias))
                            throw new Exception("An inherited repository must have an alias.");

                        parentSchema = GetSchema(schema.ParentID.Value, true);
                        if (parentSchema == null)
                            throw new Exception("Parent schema not found");

                        //if (parentSchema.DimensionList.Any())
                        //    didx = parentSchema.DimensionList.Max(x => x.DIdx) + 1;

                        using (var context = new DatastoreEntities())
                        {
                            if (!context.Repository.Any(x => x.UniqueKey == schema.ParentID && x.ParentId == null))
                                throw new Exception("Cannot create an repository from a non-base parent");
                            schema = parentSchema.Merge(schema);
                        }
                    }

                    #region Setup DIdx
                    foreach (var d in schema.DimensionList.Where(x => x.DIdx == 0).ToList())
                    {
                        d.DIdx = didx;
                        didx++;
                    }
                    #endregion

                    var assignedFilegroup = GetRandomFileGroup();
                    SqlHelper.AddRepository(ConfigHelper.ConnectionString, schema, assignedFilegroup);

                    timer.Stop();
                    LoggerCQ.LogDebug($"AddRepository: ID={schema.ID}, Elapsed={timer.ElapsedMilliseconds}, LockTime={q.LockTime}, WorkTime={CalcWorkTime(timer, q.LockTime)}");

                    var schema2 = GetSchema(schema.ID, true);
                    if (schema2 != null)
                    {
                        QueryCache.Clear(schema2.InternalID, schema2.ID);
                        RepositoryCacheAdd(schema2.ID);
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw new FaultException(ex.Message);
            }
        }

        internal static string GetRandomFileGroup()
        {
            var assignedFilegroup = string.Empty;
            if (_fileGroups.Count > 0)
                assignedFilegroup = _fileGroups[_rnd.Next(0, _fileGroups.Count)];
            return assignedFilegroup;
        }

        public ActionDiagnostics DeleteItems(RepositorySchema schema, IEnumerable<DataItem> list)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            if (schema == null) throw new FaultException("Schema was not specified.");
            var retval = new ActionDiagnostics { RepositoryId = schema.ID, IsSuccess = false };
            var errorList = new List<string>();
            var schema1 = GetSchema(schema.ID);
            if (schema1 == null)
            {
                errorList.Add($"Repository not found: {schema.ID}");
                return retval;
            }
            try
            {
                if (!RepositoryExists(schema.ID))
                {
                    LoggerCQ.LogWarning($"Repository not found: {schema.ID}");
                    //errorList.Add("The repository has not been initialized! ID: " + schema.ID);
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(schema.ID);
                }
                else if (schema.VersionHash != GetSchemaHash(schema1.ID))
                {
                    //If version mismatch then log
                    SchemaMismatchDebug(schema, schema1);
                    throw new Gravitybox.Datastore.Common.Exceptions.SchemaVersionException();
                }
                else
                {
                    var itemCount = list.Count();
                    schema = schema1;
                    var timer = Stopwatch.StartNew();
                    var lockTime = 0;
                    var waitingLocks = 0;
                    var readLockCount = 0;
                    var count = 0;
                    using (var q = new AcquireReaderLock(schema.ID, "DeleteItems"))
                    {
                        waitingLocks = q.WaitingLocksOnEntry;
                        readLockCount = q.ReadLockCount;
                        var results = SqlHelper.DeleteData(schema, list, ConfigHelper.ConnectionString);
                        count = results.AffectedCount;
                        schema = GetSchema(schema.ID);

                        //Only clear the cache if something happened
                        if (count > 0)
                            QueryCache.Clear(schema.InternalID, schema.ID);

                        timer.Stop();
                        retval.ComputeTime = timer.ElapsedMilliseconds;
                        lockTime = q.LockTime;
                        retval.LockTime = q.LockTime;
                        retval.IsSuccess = true;
                        retval.Count = itemCount;
                    }

                    _system.LogRepositoryPerf(new RepositorySummmaryStats
                    {
                        ActionType = RepositoryActionConstants.DeleteData,
                        Elapsed = (int)timer.ElapsedMilliseconds,
                        LockTime = lockTime,
                        RepositoryId = schema.ID,
                        ItemCount = itemCount,
                        WaitingLocksOnEntry = waitingLocks,
                        ReadLockCount = readLockCount,
                    });

                    LoggerCQ.LogDebug($"DeleteItems: ID={schema.ID}" +
                        $", Elapsed={timer.ElapsedMilliseconds}" +
                        $", LockTime={lockTime}" +
                        $", WorkTime={CalcWorkTime(timer, lockTime)}" +
                        (waitingLocks > 0 ? ", WaitLocks=" + waitingLocks : string.Empty) +
                        $", Cached={(itemCount - count)}" +
                        $", Count={itemCount}");
                }
                return retval;
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"DeleteItems failed: RepositoryId={schema.ID}");
                throw new FaultException(ex.Message);
            }
        }

        public ActionDiagnostics DeleteData(RepositorySchema schema, DataQuery query)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            var retval = new ActionDiagnostics { RepositoryId = schema.ID, IsSuccess = false };
            var errorList = new List<string>();
            var schema1 = GetSchema(schema.ID);
            if (schema1 == null)
            {
                errorList.Add($"Repository not found: {schema.ID}");
                return retval;
            }
            var timer = Stopwatch.StartNew();
            try
            {
                var lockTime = 0;
                if (!RepositoryExists(schema.ID))
                {
                    LoggerCQ.LogWarning($"Repository not found: {schema.ID}");
                    //errorList.Add("The repository has not been initialized! ID: " + schema.ID);
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(schema.ID);
                }
                else if (schema.VersionHash != GetSchemaHash(schema1.ID))
                {
                    //If version mismatch then log
                    SchemaMismatchDebug(schema, schema1);
                    throw new Gravitybox.Datastore.Common.Exceptions.SchemaVersionException();
                }
                else
                {
                    schema = schema1;
                    var count = 0;
                    var readLockCount = 0;
                    var waitingLocks = 0;
                    using (var q = new AcquireWriterLock(schema.ID, "DeleteData"))
                    {
                        waitingLocks = q.WaitingLocksOnEntry;
                        readLockCount = q.ReadLockCount;
                        using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                        {
                            lockTime = q.LockTime;
                            schema = GetSchema(schema.ID);
                            var dimensionList = _dimensionCache.Get(context, schema, schema.InternalID, new List<DataItem>());
                            var results = SqlHelper.DeleteData(schema, query, dimensionList, ConfigHelper.ConnectionString);
                            count = results.AffectedCount;

                            //Only clear the cache if something happened
                            if (count > 0)
                                QueryCache.Clear(schema.InternalID, schema.ID);
                        }
                        timer.Stop();
                        retval.ComputeTime = timer.ElapsedMilliseconds;
                        retval.LockTime = q.LockTime;
                        retval.IsSuccess = true;
                        retval.Count = count;
                    }

                    _system.LogRepositoryPerf(new RepositorySummmaryStats
                    {
                        ActionType = RepositoryActionConstants.DeleteData,
                        Elapsed = (int)timer.ElapsedMilliseconds,
                        LockTime = lockTime,
                        RepositoryId = schema.ID,
                        ItemCount = count,
                        WaitingLocksOnEntry = waitingLocks,
                        ReadLockCount = readLockCount,
                    });

                    LoggerCQ.LogDebug("DeleteData: ID=" + schema.ID +
                        $", Elapsed={timer.ElapsedMilliseconds}" +
                        $", LockTime={lockTime}" +
                        $", WorkTime={CalcWorkTime(timer, lockTime)}" +
                        (waitingLocks > 0 ? ", WaitLocks=" + waitingLocks : string.Empty) +
                        $", Count={count}" +
                        $", QueryString=\"{query.ToString()}\"");

                }
                return retval;
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"DeleteData failed: RepositoryId={schema.ID}");
                throw new FaultException(ex.Message);
            }
            finally
            {
                //If takes too long then mark for statistics refresh
                if (timer.ElapsedMilliseconds > TableStatsMaintenace.StatCheckThreshold)
                    _statsMaintenance.MarkRefreshStats(schema.ID);
            }
        }

        public ActionDiagnostics Clear(Guid repositoryId)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            var retval = new ActionDiagnostics { RepositoryId = repositoryId, IsSuccess = false };
            var errorList = new List<string>();
            var timer = Stopwatch.StartNew();
            var lockTime = 0;
            try
            {
                if (!RepositoryExists(repositoryId))
                {
                    LoggerCQ.LogWarning($"Repository not found: {repositoryId}");
                    //errorList.Add("The repository has not been initialized! ID: " + repositoryId);
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    using (var q = new AcquireWriterLock(repositoryId, "Clear"))
                    {
                        lockTime = q.LockTime;
                        var schema = GetSchema(repositoryId);
                        retval.Count = SqlHelper.Clear(schema, ConfigHelper.ConnectionString);
                        QueryCache.Clear(schema.InternalID, schema.ID);
                        retval.LockTime = q.LockTime;
                        retval.IsSuccess = true;
                    }
                }

                timer.Stop();
                retval.ComputeTime = timer.ElapsedMilliseconds;
                LoggerCQ.LogDebug($"Clear: ID={repositoryId}, Elapsed={timer.ElapsedMilliseconds}, LockTime={lockTime}, WorkTime={CalcWorkTime(timer, lockTime)}");
                return retval;
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"Clear failed: RepositoryId={repositoryId}");
                throw new FaultException(ex.Message);
            }
        }

        private static readonly ConcurrentDictionary<string, DateTime> _runningQueries = new ConcurrentDictionary<string, DateTime>();

        public DataQueryResults Query(Guid repositoryId, DataQuery query)
        {
            return Query(repositoryId, query, false);
        }

        public byte[] QueryAndStream(Guid repositoryId, DataQuery query)
        {
            var v = Query(repositoryId, query, false);
            return v.ObjectToBin();
        }

        internal DataQueryResults Query(Guid repositoryId, DataQuery query, bool isInternal = false)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            DataQueryResults retval = null;
            if (query == null) return null;
            var queryString = query.ToString();
            var queryKey = $"{queryString}|{repositoryId}";

            #region Log
            if (!isInternal)
            {
                LoggerCQ.LogTrace($"Query begin: ID={repositoryId}" +
                                  $", PO={query.PageOffset}, RPP={query.RecordsPerPage}" +
                                  $", QueryString=\"{queryString}\"");
            }
            #endregion

            var lockTime = 0;
            var timer = Stopwatch.StartNew();
            try
            {
                //If the exact same query is running then wait until it is cached and then process this one
                var runningQueryWaitTime = CheckRunningQuery(repositoryId, query, queryKey);

                var waitingLocks = 0;
                var readLockCount = 0;

                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning($"Repository not found: {repositoryId}");
                    //retval.ErrorList = new string[] { "The repository has not been initialized! ID: " + repositoryId };
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    string executeHistory = string.Empty;
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        //Do not get a reader lock if using cache. this was causing performance issues in production
                        var id = schema.InternalID;
                        var isCore = false;
                        if (ConfigHelper.AllowCaching) //only check if allow caching. this is used for debugging/performance testing
                            retval = QueryCache.Get(context, query, id, schema.ID, out isCore);
                        var cacheHit = (retval != null) && !isCore;
                        if (isInternal)
                        {
                            cacheHit = false;
                            isCore = false;
                        }
                        var recordMultiplier = 0;
                        var originalQuery = Utilities.Clone(query);
                        //If there was no cache hit or just a core hit then have to hit the DB
                        if (!cacheHit || isCore)
                        {
                            using (var q = new AcquireReaderLock(repositoryId, "Query"))
                            {
                                waitingLocks = q.WaitingLocksOnEntry;
                                readLockCount = q.ReadLockCount;
                                var dimensionList = _dimensionCache.Get(context, schema, id, new List<DataItem>());
                                var allDimensions = new List<DimensionItem>();

                                //Run this async to find all dimensions
                                //This will be tacked on the end of the query before returning
                                //If the NoDimensions flag is on then there is no need to calculate the AllDimensions list either
                                Task allDimTask = null;
                                if (query.IncludeAllDimensions)
                                {
                                    allDimTask = Task.Factory.StartNew(() =>
                                    {
                                        allDimensions = GetAllDimensions(query, id, context, schema, dimensionList);
                                    });
                                }

                                //If this query matches the core results (dimensions/count)
                                //then pull them out so we do not have the requery these values
                                DataQueryResults coreResults = null;
                                if (isCore)
                                {
                                    query.ExcludeCount = true;
                                    query.IncludeDimensions = false;
                                    coreResults = Utilities.Clone(retval);
                                }

                                lockTime = q.LockTime;
                                #region Actually Hit Database

                                //If the RPP is even and <= 100 then we load more records than necessary and parse into N+1, N+2 buckets
                                //so the next sequential call will be already cached
                                //On first page do NOT cache if any filters
                                var forwardLoad = false;
                                var origRpp = query.RecordsPerPage;
                                var hasGeo = query.FieldFilters.Any(x => x is GeoCodeFieldFilter);
                                var hasKeyword = !string.IsNullOrEmpty(query.Keyword);
                                //If single select with filters will be on page 1. If > first page then assume we are paging larger set
                                var hasFilters = query.FieldFilters.Any() && (query.PageOffset == 1);
                                var isMasterResults = (query.NonParsedFieldList["masterresults"] == "true" || query.NonParsedFieldList["masterresults"] == "1");
                                //If there are no records then do not look ahead
                                if (!isMasterResults && !hasFilters && !hasGeo && !hasKeyword && query.RecordsPerPage <= 100 && query.IncludeRecords)
                                {
                                    //Only look ahead if there are a small amount of records to cache
                                    if (query.RecordsPerPage <= 100)
                                        recordMultiplier = 3;
                                    forwardLoad = true;
                                    retval = SqlHelper.Query(schema, id, query, dimensionList, out executeHistory, query.RecordsPerPage * (recordMultiplier - 1));
                                    //If this was cache hit but only find a core match then need to replace the records
                                }
                                else
                                {
                                    retval = SqlHelper.Query(schema, id, query, dimensionList, out executeHistory);
                                }

                                //This is the version of the data in this repository
                                //the data version changes only when data is changed in the repository
                                retval.DataVersion = GetRepositoryChangeStamp(context, schema.InternalID);

                                //Setup core results if necesary
                                //Use the core results if they are present to avoid extra calculation
                                if (isCore)
                                {
                                    query = Utilities.Clone(originalQuery);
                                    if (originalQuery.IncludeDimensions)
                                        retval.DimensionList = coreResults.DimensionList;
                                    if (!originalQuery.ExcludeCount)
                                        retval.TotalRecordCount = coreResults.TotalRecordCount;
                                }

                                if (forwardLoad)
                                {
                                    //Clone all N results at the same time
                                    //Then we pop them off the stack in the loop below
                                    var clonedResultsCache = new ConcurrentStack<DataQueryResults>();
                                    var clonedQueryCache = new ConcurrentStack<DataQuery>();
                                    var cloneTasks = new List<Task>();
                                    for (var ii = 0; ii < recordMultiplier; ii++)
                                    {
                                        cloneTasks.Add(Task.Factory.StartNew(() =>
                                        {
                                            clonedResultsCache.Push(Utilities.Clone(retval));
                                            clonedQueryCache.Push(Utilities.Clone(originalQuery));
                                        }));
                                    }
                                    Task.WaitAll(cloneTasks.ToArray());

                                    Task firstTask = null;
                                    var otherTasks = new List<Task>();
                                    for (var ii = 0; ii < recordMultiplier; ii++)
                                    {
                                        //These operations can be done async for all [2..N] tasks
                                        //only the first one needs to be complete to return.
                                        //The other look forward operations can cache async
                                        var tempii = ii;
                                        var task = Task.Factory.StartNew(() =>
                                        {
                                            DataQueryResults newR;
                                            clonedResultsCache.TryPop(out newR); //Utilities.Clone(retval);
                                            DataQuery q2;
                                            clonedQueryCache.TryPop(out q2); //Utilities.Clone(originalQuery);
                                            q2.PageOffset = q2.PageOffset + tempii;
                                            q2.RecordsPerPage = origRpp;
                                            newR.Query = q2;
                                            if (tempii > 0) newR.ComputeTime = 0; //only the first one logs the time
                                            newR.RecordList = newR.RecordList.Skip(origRpp * tempii).Take(origRpp).ToList();
                                            if (tempii == 0 || newR.RecordList.Count != 0)
                                            {
                                                allDimTask?.Wait(SqlHelper.ThreadTimeout); //ensure this calculation is completed before use
                                                newR.AllDimensionList = allDimensions;
                                                if (!isInternal)
                                                    QueryCache.Set(context, q2, id, schema.ID, newR);
                                            }
                                        });
                                        if (ii == 0) firstTask = task; //this must be finished before results are returned
                                        else otherTasks.Add(task);
                                    }
                                    firstTask.Wait(SqlHelper.ThreadTimeout); //wait for first task, the others can take their time
                                    allDimTask?.Wait(SqlHelper.ThreadTimeout); //ensure this calculation is completed before use
                                    retval.AllDimensionList = allDimensions;
                                    //Get the record list for the query provided.
                                    //We must remove the look ahead records from here (if exists)
                                    retval.RecordList = retval.RecordList.Take(origRpp).ToList();
                                }
                                else
                                {
                                    //Use the core results if they are present to avoid extra calculation
                                    if (isCore)
                                    {
                                        retval.Query = query;
                                    }
                                    allDimTask?.Wait(SqlHelper.ThreadTimeout); //ensure this calculation is completed before use
                                    retval.AllDimensionList = allDimensions;
                                    if (!isInternal)
                                        QueryCache.Set(context, query, id, schema.ID, retval);
                                }
                                #endregion
                            } //Query Lock
                        }
                        else
                        {
                            retval.CacheHit = true;
                        }
                        retval.LockTime = lockTime;

                        timer.Stop();
                        #region Log
                        if (!isInternal)
                        {
                            var logMsg = "Query: ID=" + repositoryId +
                            ", Cache=" + (cacheHit ? "1" : "0") +
                            ", CoreHit=" + (isCore ? "1" : "0") +
                            ", Elapsed=" + timer.ElapsedMilliseconds +
                            ", LockTime=" + lockTime +
                            ", WorkTime=" + CalcWorkTime(timer, lockTime) +
                            (waitingLocks > 0 ? ", WaitLocks=" + waitingLocks : string.Empty) +
                            ", PO=" + query.PageOffset + ", RPP=" + query.RecordsPerPage +
                            ", Count=" + retval.RecordList.Count +
                            ", Total=" + retval.TotalRecordCount +
                            (runningQueryWaitTime > 0 ? ", WaitTime=" + runningQueryWaitTime : string.Empty) +
                            (recordMultiplier > 0 ? ", RecordMultiplier=" + recordMultiplier : "") +
                            (string.IsNullOrEmpty(executeHistory) ? string.Empty : ", EH=" + executeHistory);
                            logMsg += $", QueryString=\"{queryString}\"";
                            LoggerCQ.LogDebug(logMsg);
                        }
                        #endregion

                        if (!isInternal)
                        {
                            var logItem = new Gravitybox.Datastore.EFDAL.Entity.RepositoryLog
                            {
                                IPAddress = query.IPMask + string.Empty,
                                RepositoryId = schema.InternalID,
                                Count = retval.RecordList.Count,
                                ElapsedTime = (int)timer.ElapsedMilliseconds,
                                LockTime = lockTime,
                                UsedCache = cacheHit,
                                QueryId = new Guid(query.QueryID),
                            };
                            logItem.SetValue(Gravitybox.Datastore.EFDAL.Entity.RepositoryLog.FieldNameConstants.Query, queryString, true);
                            _queryLogManager.Log(logItem);
                        }

                    } //context

                    _system.LogRepositoryPerf(new RepositorySummmaryStats
                    {
                        ActionType = RepositoryActionConstants.Query,
                        Elapsed = (int)timer.ElapsedMilliseconds,
                        LockTime = lockTime + runningQueryWaitTime,
                        RepositoryId = repositoryId,
                        ItemCount = retval.RecordList.Count,
                        WaitingLocksOnEntry = waitingLocks,
                        ReadLockCount = readLockCount,
                    });

                    return retval;
                }
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (FaultException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (ex is DatastoreException)
                    LoggerCQ.LogError($"Query failed: RepositoryId={repositoryId}, QueryString=\"{query.ToString()}\", Elapsed={timer.ElapsedMilliseconds}, Error={ex.Message}");
                else
                    LoggerCQ.LogError(ex, $"Query failed: RepositoryId={repositoryId}, QueryString=\"{query.ToString()}\", Elapsed={timer.ElapsedMilliseconds}");
                throw new FaultException(ex.Message);
            }
            finally
            {
                DateTime d;
                var b = _runningQueries.TryRemove(queryKey, out d);
                if (!b) LoggerCQ.LogDebug("Running query dequeue failed");

                //If takes too long then mark for statistics refresh (real execute excludes lock time)
                if ((timer.ElapsedMilliseconds - lockTime) > TableStatsMaintenace.StatCheckThreshold)
                    _statsMaintenance.MarkRefreshStats(repositoryId);
            }

        }

        /// <summary>
        /// If the exact same query is running then wait until it is cached and then process this one
        /// </summary>
        private int CheckRunningQuery(Guid repositoryId, DataQuery query, string queryKey)
        {
            var timer = Stopwatch.StartNew();
            var waitTime = 0;
            var waitLoops = 0;
            while (!_runningQueries.TryAdd(queryKey, DateTime.Now))
            {
                if (waitLoops == 0) System.Threading.Thread.Sleep(10);
                else if (waitLoops == 1) System.Threading.Thread.Sleep(20);
                else System.Threading.Thread.Sleep(30);
                waitLoops++;
            }

            //Log if there was a wait.
            if (waitLoops > 0)
            {
                waitTime = (int)timer.ElapsedMilliseconds;
                LoggerCQ.LogDebug($"Query Wait, ID={repositoryId}, Elapsed={timer.ElapsedMilliseconds}, WaitLoops={waitLoops}, WaitCount={_runningQueries.Count}, Query=\"{query.ToString()}\"");
            }
            return waitTime;
        }

        public int GetLastTimestamp(Guid repositoryId, DataQuery query)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            try
            {
                var timer = Stopwatch.StartNew();

                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning($"Repository not found: {repositoryId}");
                    //retval.ErrorList = new string[] { "The repository has not been initialized! ID: " + repositoryId };
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        using (var q = new AcquireReaderLock(repositoryId, "GetLastTimestamp"))
                        {
                            var id = schema.InternalID;
                            var dimensionList = _dimensionCache.Get(context, schema, id, new List<DataItem>());
                            var retval = SqlHelper.GetLastTimestamp(schema, id, query, dimensionList);
                            timer.Stop();
                            LoggerCQ.LogDebug("GetLastTimestamp: ID=" + repositoryId + ", Elapsed=" + timer.ElapsedMilliseconds + ", QueryString=\"" + query.ToString() + "\"");
                            return retval;
                        }
                    }
                }
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                throw new FaultException(ex.Message);
            }
        }

        public int GetTimestamp()
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            return Utilities.CurrentTimestamp;
        }

        public void ShutDown()
        {
            _queryLogManager.Empty();
        }

        /// <summary>
        /// Returns the number of items in the repository
        /// </summary>
        /// <returns></returns>
        public long GetItemCount(Guid repositoryId)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            LoggerCQ.LogTrace($"GetItemCount begin: RepositoryId={repositoryId}");
            try
            {
                using (var q = new AcquireReaderLock(repositoryId, "GetItemCount"))
                {
                    var schema = GetSchema(repositoryId);
                    if (schema == null) return 0;
                    return SqlHelper.Count(schema, ConfigHelper.ConnectionString);
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"GetItemCount failed: RepositoryId={repositoryId}");
                throw new FaultException(ex.Message);
            }
        }

        internal static void ClearSchemaCache(Guid repositoryId)
        {
            _schemaCache.ClearSchemaCache(repositoryId);
        }
        
        internal static long GetSchemaHash(Guid repositoryId)
        {
            return _schemaCache.GetSchemaHash(repositoryId);
        }

        internal static int? GetSchemaParentId(int repositoryId)
        {
            return _schemaCache.GetSchemaParentId(repositoryId);
        }

        internal static RepositorySchema GetSchema(Guid repositoryId)
        {
            return GetSchema(repositoryId, false) ?? throw new FaultException($"The repository does not exist. RepositoryId={repositoryId}");
        }

        internal static RepositorySchema GetSchema(Guid repositoryId, bool clear)
        {
            using (new PerformanceLogger($"GetSchema: RepositoryId={repositoryId}, Clear={clear}"))
            {
                return _schemaCache.GetSchema(repositoryId, clear);
            }
        }

        public bool IsValidFormat(Guid repositoryId, DataItem item)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            try
            {
                if (!RepositoryExists(repositoryId))
                {
                    return false;
                }
                else
                {
                    using (var q = new AcquireReaderLock(repositoryId, "IsValidFormat"))
                    {
                        var schema = GetSchema(repositoryId);
                        if (item.ItemArray?.Length != schema.FieldList.Count) return false;

                        var index = 0;
                        foreach (var field in schema.FieldList)
                        {
                            if (item.ItemArray[index] != null)
                            {
                                switch (field.DataType)
                                {
                                    case RepositorySchema.DataTypeConstants.Bool:
                                        if (!(item.ItemArray[index] is bool)) return false;
                                        break;
                                    case RepositorySchema.DataTypeConstants.DateTime:
                                        if (!(item.ItemArray[index] is DateTime)) return false;
                                        break;
                                    case RepositorySchema.DataTypeConstants.Float:
                                        if (!(item.ItemArray[index] is double)) return false;
                                        break;
                                    case RepositorySchema.DataTypeConstants.GeoCode:
                                        if (!(item.ItemArray[index] is GeoCode)) return false;
                                        break;
                                    case RepositorySchema.DataTypeConstants.Int:
                                        if (!(item.ItemArray[index] is int)) return false;
                                        break;
                                    case RepositorySchema.DataTypeConstants.Int64:
                                        if (!(item.ItemArray[index] is long)) return false;
                                        break;
                                    case RepositorySchema.DataTypeConstants.String:
                                        if (!(item.ItemArray[index] is string)) return false;
                                        break;
                                    //case RepositorySchema.DataTypeConstants.List:
                                    //    if (!(item.ItemArray[index] is string[])) return false;
                                    //    break;
                                    default:
                                        LoggerCQ.LogWarning($"IsItemValid: Unknown data type: {field.DataType}");
                                        throw new Exception("Unknown data type!");
                                }
                            }
                            index++;
                        }
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"IsValidFormat failed: RepositoryId={repositoryId}");
                throw new FaultException(ex.Message);
            }
        }

        internal static bool RepositoryExists(Guid repositoryId)
        {
            const int MaxTry = 4;
            var tryCount = 1;
            var timer = Stopwatch.StartNew();
            do
            {
                var theValue = false;
                var cacheHit = false;
                try
                {
                    //Cache whether a repository exists
                    theValue = _repositoryExistCache.Contains(repositoryId);
                    if (theValue)
                    {
                        cacheHit = true;
                        return theValue;
                    }

                    //If made it here then need to hit database
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        theValue = context.Repository.Any(x => x.UniqueKey == repositoryId && !x.IsDeleted && x.IsInitialized);
                        if (theValue) _repositoryExistCache.Add(repositoryId);
                        else _repositoryExistCache.Remove(repositoryId);
                        return theValue;
                    }

                }
                catch (System.Data.SqlClient.SqlException ex)
                {
                    if (ex.Message.ToLower().Contains("deadlock"))
                    {
                        LoggerCQ.LogWarning(ex, $"RepositoryExists deadlock {tryCount}: RepositoryId={repositoryId}");
                    }
                    else
                    {
                        LoggerCQ.LogError(ex, $"RepositoryExists failed: RepositoryId={repositoryId}");
                        throw;
                    }
                    System.Threading.Thread.Sleep(_rnd.Next(150, 500));
                }
                catch (Exception ex)
                {
                    LoggerCQ.LogError(ex, $"RepositoryExists failed: RepositoryId={repositoryId}");
                    throw;
                }
                finally
                {
                    timer.Stop();
                    LoggerCQ.LogTrace($"RepositoryExists: ID={repositoryId}, CacheHit={cacheHit}, Value={theValue}, TryCount={tryCount}, Elapsed={timer.ElapsedMilliseconds}");
                }
            } while (++tryCount <= MaxTry);
            throw new Exception("Cannot complete operation.");
        }

        private void RepositoryCacheRemove(Guid repositoryId)
        {
            _repositoryExistCache.Remove(repositoryId);
        }

        private void RepositoryCacheAdd(Guid repositoryId)
        {
            _repositoryExistCache.Add(repositoryId);
        }

        public ActionDiagnostics UpdateData(RepositorySchema schema, IEnumerable<DataItem> list)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            var retval = new ActionDiagnostics { RepositoryId = schema.ID, IsSuccess = false };
            if (list == null || !list.Any())
            {
                retval.IsSuccess = true;
                return retval;
            }

            LoggerCQ.LogTrace($"UpdateData begin: RepositoryId={schema.ID}");
            
            var errorList = new List<string>();
            var schema1 = GetSchema(schema.ID);
            if (schema1 == null)
            {
                LoggerCQ.LogWarning($"Repository not found: {schema.ID}");
                throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(schema.ID);
            }
            
            var timer = Stopwatch.StartNew();
            try
            {
                using (var q = new AcquireWriterLock(schema.ID, TraceInfoUpdateData))
                {
                    if (schema.VersionHash != GetSchemaHash(schema1.ID))
                    {
                        //If version mismatch then log
                        SchemaMismatchDebug(schema, schema1);
                        throw new Gravitybox.Datastore.Common.Exceptions.SchemaVersionException();
                    }
                    else
                    {
                        schema = schema1;
                        var itemCount = list.Count();
                        using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                        {
                            var id = schema.InternalID;
                            var dimensionList = _dimensionCache.Get(context, schema, id, list);

                            var results = SqlHelper.UpdateData(schema, dimensionList, list, ConfigHelper.ConnectionString);
                            timer.Stop();
                            retval.LockTime = q.LockTime;
                            retval.ComputeTime = timer.ElapsedMilliseconds;
                            retval.IsSuccess = true;
                            retval.Count = itemCount;
                            LoggerCQ.LogDebug($"UpdateData: ID={schema.ID}" +
                                $", Elapsed={timer.ElapsedMilliseconds}" +
                                $", LockTime={q.LockTime}" +
                                $", WorkTime={CalcWorkTime(timer, q.LockTime)}" +
                                (q.WaitingLocksOnEntry > 0 ? ", WaitLocks=" + q.WaitingLocksOnEntry : string.Empty) +
                                $", Found={results.FountCount}" +
                                $", Cached={(itemCount - results.AffectedCount)}" +
                                $", Count={itemCount}");

                            if (results.AffectedCount > 0)
                                QueryCache.Clear(id, schema.ID);

                            _system.LogRepositoryPerf(new RepositorySummmaryStats
                            {
                                ActionType = RepositoryActionConstants.SaveData,
                                Elapsed = (int)timer.ElapsedMilliseconds,
                                LockTime = q.LockTime,
                                RepositoryId = schema.ID,
                                ItemCount = itemCount,
                                WaitingLocksOnEntry = q.WaitingLocksOnEntry,
                                ReadLockCount = q.ReadLockCount,
                            });

                        }
                    }
                }
                return retval;
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"UpdateData failed: RepositoryId={schema.ID}, Elapsed={timer.ElapsedMilliseconds}");
                throw new FaultException(ex.Message);
            }
            finally
            {
                //If takes too long then mark for statistics refresh
                if (timer.ElapsedMilliseconds > TableStatsMaintenace.StatCheckThreshold)
                    _statsMaintenance.MarkRefreshStats(schema.ID);
            }
        }

        #region UpdateDataWhereAsync
        private readonly System.Timers.Timer _timerUpdateDataWhereAsync;
        private readonly ConcurrentQueue<UpdateDataWhereCacheItem> _updateDataWhereQueue = new ConcurrentQueue<UpdateDataWhereCacheItem>();
        public void UpdateDataWhereAsync(RepositorySchema schema, DataQuery query, IEnumerable<DataFieldUpdate> list)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            try
            {
                _updateDataWhereQueue.Enqueue(new UpdateDataWhereCacheItem
                {
                    list = list,
                    query = query,
                    schema = schema,
                });
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"UpdateDataWhereAsync enqueue failed: RepositoryId={schema.ID}, Query=\"{query.ToString()}\"");
                throw new FaultException(ex.Message);
            }
        }

        private void _timerUpdateDataWhereAsync_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            try
            {
                _timerUpdateDataWhereAsync.Stop();
                UpdateDataWhereCacheItem item;
                var index = 0;
                do
                {
                    if (_updateDataWhereQueue.TryDequeue(out item))
                    {
                        var result = UpdateDataWhere(item.schema, item.query, item.list);
                        index++;
                    }
                } while (item != null);

                if (index > 0)
                    LoggerCQ.LogDebug("UpdateDataWhereAsync Processed: Count=" + index);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, "UpdateDataWhereAsync timer event failed");
            }
            finally
            {
                _timerUpdateDataWhereAsync.Start();
            }
        }
        #endregion

        public ActionDiagnostics UpdateDataWhere(RepositorySchema schema, DataQuery query, IEnumerable<DataFieldUpdate> list)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();
            
            var retval = new ActionDiagnostics { RepositoryId = schema.ID, IsSuccess = false };
            if (list == null || !list.Any())
            {
                retval.IsSuccess = true;
                return retval;
            }

            LoggerCQ.LogTrace($"UpdateDataWhere begin: RepositoryId={schema.ID}, Query=\"{query.ToString()}\"");

            var schema1 = GetSchema(schema.ID);
            if (schema1 == null)
            {
                LoggerCQ.LogWarning($"Repository not found: {schema.ID}");
                throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(schema.ID);
            }

            var timer = Stopwatch.StartNew();
            var errorList = new List<string>();
            try
            {
                using (var q = new AcquireReaderLock(schema.ID, TraceInfoUpdateData))
                {
                    if (schema.VersionHash != GetSchemaHash(schema1.ID))
                    {
                        //If version mismatch then log
                        SchemaMismatchDebug(schema, schema1);
                        throw new Gravitybox.Datastore.Common.Exceptions.SchemaVersionException();
                    }
                    else
                    {
                        schema = schema1;
                        using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                        {
                            var id = schema.InternalID;
                            var dimensionList = _dimensionCache.Get(context, schema, id, list);

                            var results = SqlHelper.UpdateData(schema, query, dimensionList, list, ConfigHelper.ConnectionString);
                            DataManager.Add(schema.ID);
                            timer.Stop();
                            retval.LockTime = q.LockTime;
                            retval.ComputeTime = timer.ElapsedMilliseconds;
                            retval.IsSuccess = true;
                            retval.Count = results.AffectedCount;
                            LoggerCQ.LogDebug($"UpdateDataWhere: ID={schema.ID}" +
                                $", Elapsed={timer.ElapsedMilliseconds}" +
                                $", LockTime={q.LockTime}" +
                                $", WorkTime={CalcWorkTime(timer, q.LockTime)}" +
                                (q.WaitingLocksOnEntry > 0 ? ", WaitLocks=" + q.WaitingLocksOnEntry : string.Empty) +
                                $", Count={results.AffectedCount}" +
                                $", Fields={string.Join("|", list.Where(x => x != null).Select(x => x.FieldName))}" +
                                $", QueryString=\"{query.ToString()}\"");
                            QueryCache.Clear(id, schema.ID);

                            _system.LogRepositoryPerf(new RepositorySummmaryStats
                            {
                                ActionType = RepositoryActionConstants.SaveData,
                                Elapsed = (int)timer.ElapsedMilliseconds,
                                LockTime = q.LockTime,
                                RepositoryId = schema.ID,
                                ItemCount = results.AffectedCount,
                                WaitingLocksOnEntry = q.WaitingLocksOnEntry,
                                ReadLockCount = q.ReadLockCount,
                            });

                        }
                    }
                }
                return retval;
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"UpdateDataWhere failed: RepositoryId={schema.ID}, Query=\"{query.ToString()}\"");
                throw new FaultException(ex.Message);
            }

        }

        public ActionDiagnostics UpdateSchema(RepositorySchema newSchema)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            return UpdateSchema(newSchema, false);
        }

        internal ActionDiagnostics UpdateSchema(RepositorySchema newSchema, bool extremeVerify)
        {
            var retval = new ActionDiagnostics { IsSuccess = false };
            if (newSchema == null)
            {
                throw new RepositoryNotInitializedException(Guid.Empty);
            }
            retval.RepositoryId = newSchema.ID;

            LoggerCQ.LogTrace($"UpdateSchema begin: RepositoryId={newSchema.ID}, ExtremeVerify={extremeVerify}");
            var otherLockList = new List<AcquireWriterLock>();
            try
            {
                var timer = Stopwatch.StartNew();
                using (var q = new AcquireWriterLock(newSchema.ID, "UpdateSchema"))
                {
                    //Lock parent if one
                    if (newSchema.ParentID != null)
                    {
                        otherLockList.Add(new AcquireWriterLock(newSchema.ParentID.Value, "UpdateSchema-Parent"));
                    }
                    else
                    {
                        var children = GetChildRepositories(newSchema.ID);
                        children.ForEach(x => otherLockList.Add(new AcquireWriterLock(x, "UpdateSchema-Child")));
                    }

                    if (!RepositoryExists(newSchema.ID))
                    {
                        throw new RepositoryNotInitializedException(newSchema.ID);
                    }
                    else
                    {
                        var actionResults = new UpdateScheduleResults();
                        var repositoryId = 0;
                        RepositorySchema currentSchema = null;

                        using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                        {
                            repositoryId = context.Repository.First(x => x.UniqueKey == newSchema.ID).RepositoryId;
                            currentSchema = GetSchema(newSchema.ID, true);
                            var repository = context.Repository.FirstOrDefault(x => x.UniqueKey == newSchema.ID);
                            if (repository == null)
                            {
                                throw new RepositoryNotInitializedException(newSchema.ID);
                            }

                            //if (retval == null || retval.Length == 0)
                            {
                                //Update existing dimensions on the new schema
                                foreach (var item in currentSchema.DimensionList)
                                {
                                    var target = newSchema.DimensionList.FirstOrDefault(x => x.Name == item.Name);
                                    if (target != null)
                                    {
                                        target.DIdx = item.DIdx;
                                    }
                                }

                                //Find dimensions that have been removed and clean the database
                                foreach (var item in currentSchema.DimensionList)
                                {
                                    var target = newSchema.DimensionList.FirstOrDefault(x => x.Name == item.Name);
                                    if (target == null)
                                    {
                                        var sb = new StringBuilder();
                                        sb.AppendLine($"delete from [{SqlHelper.GetDimensionTableName(newSchema.ID)}] where [DIdx] = {item.DIdx}");
                                        sb.AppendLine($"delete from [{SqlHelper.GetDimensionValueTableName(newSchema.ID)}] where [DIdx] = {item.DIdx}");
                                        SqlHelper.ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), null, false);
                                    }
                                }

                                //Find dimensions on new schema that have just been added and assign a unique DIdx
                                foreach (var item in newSchema.DimensionList)
                                {
                                    var source = currentSchema.DimensionList.FirstOrDefault(x => x.Name == item.Name);
                                    if (source == null)
                                    {
                                        var allValidDim = newSchema.DimensionList.Where(x => x.DIdx > 0).ToList();
                                        long didx;
                                        if (newSchema.ParentID == null)
                                            didx = Constants.DGROUP;
                                        else
                                            didx = Constants.DGROUPEXT;
                                        if (allValidDim.Count > 0)
                                            didx = allValidDim.Max(x => x.DIdx) + 1;
                                        item.DIdx = didx;
                                    }
                                }
                            }

                            using (new PerformanceLogger($"UpdateSchema: updating SQL schema: RepositoryId={newSchema.ID}, ExtremeVerify={extremeVerify}"))
                            {
                                actionResults = SqlHelper.UpdateSchema(ConfigHelper.ConnectionString, currentSchema,
                                    newSchema, extremeVerify);
                            }
                        }

                        #region Create/Delete indexes based on fields
                        if (newSchema.ParentID == null)
                        {
                            var sbFields = new StringBuilder();
                            foreach (var field in newSchema.FieldList.Where(x => !x.IsPrimaryKey &&
                                                        x.DataType != RepositorySchema.DataTypeConstants.GeoCode &&
                                                        x.DataType != RepositorySchema.DataTypeConstants.List))
                            {
                                var dataTable = SqlHelper.GetTableName(newSchema.ID);
                                var useIndex = field.AllowIndex;
                                if (field.DataType == RepositorySchema.DataTypeConstants.String && ((field.Length <= 0) || (field.Length > 450)))
                                    useIndex = false;

                                if (useIndex)
                                {
                                    var indexName = SqlHelper.GetIndexName(field, dataTable);
                                    sbFields.AppendLine($"if not exists(select * from sys.indexes where name = '" + indexName + "')");
                                    sbFields.AppendLine($"CREATE NONCLUSTERED INDEX [" + indexName + "] ON [" + dataTable + "] ([" + field.Name + "] ASC);");
                                    sbFields.AppendLine();
                                }
                                else
                                {
                                    var indexName = SqlHelper.GetIndexName(field, dataTable);
                                    sbFields.AppendLine($"if exists(select * from sys.indexes where name = '" + indexName + "')");
                                    sbFields.AppendLine($"DROP INDEX [" + indexName + "] ON [" + dataTable + "]");
                                    sbFields.AppendLine();
                                }
                            }
                            SqlHelper.ExecuteSql(ConfigHelper.ConnectionString, sbFields.ToString(), null, false);
                        }
                        #endregion

                        if (actionResults != null && actionResults.Errors.Any())
                        {
                            retval.Errors = actionResults.Errors.ToArray();
                            return retval;
                        }

                        // re-Create a FTS index for all string columns
                        // must be in a separate transaction or "DROP FULLTEXT INDEX statement cannot be used inside a user transaction."
                        if (actionResults.FtsChanged)
                        {
                            LoggerCQ.LogTrace($"UpdateSchema: updating full text index: RepositoryId={newSchema.ID}");
                            SqlHelper.UpdateFTSIndex(ConfigHelper.ConnectionString, newSchema);
                        }

                        var isFinish = false;
                        var tryCount = 0;
                        const int maxTries = 3;
                        do
                        {
                            try
                            {
                                //Do a separate select in case timestamp has changed.
                                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                                {
                                    if (actionResults == null || actionResults.Errors.Count == 0)
                                    {
                                        var repository = context.Repository.FirstOrDefault(x => x.UniqueKey == newSchema.ID);
                                        newSchema.InternalID = repository.RepositoryId;
                                        repository.Name = newSchema.Name;
                                        repository.Changestamp = SqlHelper.GetChangeStamp();
                                        repository.VersionHash = newSchema.VersionHash;
                                        repository.DefinitionData = newSchema.ToXml();
                                        context.SaveChanges();
                                        QueryCache.Clear(repositoryId, newSchema.ID);
                                    }
                                }

                                //Find all repos that use this schema as a parent and update their definitions
                                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                                {
                                    var children = context.Repository
                                        .Where(x => x.ParentId == newSchema.InternalID)
                                        .ToList();

                                    var rebuildList = new Dictionary<RepositorySchema, RepositorySchema>();
                                    foreach (var child in children)
                                    {
                                        var childSchema = RepositorySchema.CreateFromXml(child.DefinitionData);
                                        var partSchema = childSchema.Subtract(currentSchema);
                                        var fullSchema = partSchema.Clone();
                                        fullSchema.FieldList.InsertRange(0, newSchema.FieldList);
                                        child.Changestamp = SqlHelper.GetChangeStamp();
                                        child.VersionHash = fullSchema.VersionHash;
                                        child.DefinitionData = fullSchema.ToXml();
                                        rebuildList.Add(partSchema, fullSchema);
                                    }
                                    context.SaveChanges();

                                    //Clear the schema cache for all children
                                    children.ForEach(item => ClearSchemaCache(item.UniqueKey));

                                    foreach (var k in rebuildList.Keys)
                                    {
                                        var sch1 = k;
                                        var sch2 = rebuildList[k];
                                        SqlHelper.CreateView(sch1, sch2, ConfigHelper.ConnectionString);
                                    }

                                }

                                isFinish = true;
                            }
                            catch (Exception ex)
                            {
                                if (tryCount < maxTries)
                                {
                                    LoggerCQ.LogWarning(ex, $"UpdateSchema deadlock {tryCount}: RepositoryId={repositoryId}");
                                    tryCount++;
                                    System.Threading.Thread.Sleep(_rnd.Next(150, 500));
                                }
                                else
                                {
                                    throw;
                                }
                            }
                        } while (!isFinish && tryCount < maxTries);

                        QueryCache.Clear(repositoryId, newSchema.ID);
                        _dimensionCache.Clear(repositoryId);

                        timer.Stop();
                        retval.ComputeTime = timer.ElapsedMilliseconds;
                        retval.LockTime = q.LockTime;
                        retval.IsSuccess = true;
                        retval.Errors = actionResults.Errors.ToArray();
                        retval.Count = retval.Errors.Any() ? 0 : 1;

                        var logText = $"UpdateSchema: ID={newSchema.ID}, Elapsed={timer.ElapsedMilliseconds}, LockTime={q.LockTime}, WorkTime={CalcWorkTime(timer, q.LockTime)}";
                        if (timer.ElapsedMilliseconds > 3000)
                            LoggerCQ.LogWarning(logText);
                        else
                            LoggerCQ.LogDebug(logText);

                        ClearSchemaCache(newSchema.ID);

                        return retval;
                    }
                }
            }
            catch (RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"UpdateSchema failed: RepositoryId={newSchema.ID}");
                throw new FaultException(ex.Message);
            }
            finally
            {
                //Dispose of all other locked repositories
                otherLockList.ForEach(x => x.Dispose());
            }
        }

        private readonly ConcurrentDictionary<Guid, QueryThreaded> _asyncQueryRunning = new ConcurrentDictionary<Guid, QueryThreaded>();
        public Guid QueryAsync(Guid repositoryId, DataQuery query)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            try
            {
                if (!Directory.Exists(ConfigHelper.AsyncCachePath)) return Guid.Empty;

                LoggerCQ.LogTrace($"QueryAsync begin: RepositoryId={repositoryId}, QueryString=\"{query.ToString()}\"");

                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning($"Repository not found: {repositoryId}");
                    throw new RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    var item = new QueryThreaded(_dimensionCache, schema, query);
                    _asyncQueryRunning.TryAdd(item.Key, item);
                    var myThread = new System.Threading.Thread(new System.Threading.ThreadStart(item.Run));
                    myThread.Start();
                    LoggerCQ.LogDebug($"QueryAsync: ID={repositoryId}, Key={item.Key}, QueryString=\"{query.ToString()}\"");
                    return item.Key;
                }
            }
            catch(RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (FaultException ex)
            {
                LoggerCQ.LogError($"QueryAsync failed: RepositoryId={repositoryId}, Query=\"{query.ToString()}\", Error={ex.Message}");
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"QueryAsync failed: RepositoryId={repositoryId}, Query=\"{query.ToString()}\"");
                throw new FaultException(ex.Message);
            }
        }

        public bool QueryAsyncReady(Guid key)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            LoggerCQ.LogTrace($"QueryAsyncReady begin: Key={key}");
            
            try
            {
                if (!_asyncQueryRunning.TryGetValue(key, out QueryThreaded qthread))
                    return false;
                if (!qthread.IsComplete)
                    return false;

                //TODO: Verify that file is fully written!!!!!
                if (!Directory.Exists(ConfigHelper.AsyncCachePath)) return false;
                var errorFileName = Path.Combine(ConfigHelper.AsyncCachePath, key.ToString() + ".error");
                if (File.Exists(errorFileName))
                {
                    LoggerCQ.LogError($"QueryAsyncReady error: Key={key}");
                    return true;
                }

                var fileName = Path.Combine(ConfigHelper.AsyncCachePath, key.ToString() + ".zzz");
                var result = File.Exists(fileName);
                LoggerCQ.LogDebug($"QueryAsyncReady: Key={key}, Result={result}");
                return result;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"QueryAsyncReady failed: Key={key}");
                throw new FaultException(ex.Message);
            }
        }

        public byte[] QueryAsyncDownload(Guid key, long chunk)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            LoggerCQ.LogTrace($"QueryAsyncDownload begin: Key={key}, Chunk={chunk}");

            if (!_asyncQueryRunning.TryGetValue(key, out QueryThreaded qthread))
                return null;
            if (!qthread.IsComplete)
                return null;

            var queryString = qthread.Query.ToString();
            try
            {
                if (chunk < 0) return null;

                if (!Directory.Exists(ConfigHelper.AsyncCachePath)) return null;
                var fileName = Path.Combine(ConfigHelper.AsyncCachePath, key.ToString() + ".zzz");

                var errorFileName = Path.Combine(ConfigHelper.AsyncCachePath, key.ToString() + ".error");
                if (File.Exists(errorFileName))
                {
                    LoggerCQ.LogDebug($"QueryAsyncDownload Error: Key={key}, Chunk={chunk}");
                    if (File.Exists(fileName)) File.Delete(fileName);
                    File.Delete(errorFileName);
                    _asyncQueryRunning.TryRemove(key, out QueryThreaded z);
                    return null;
                }
                else
                {
                    if (!File.Exists(fileName))
                    {
                        LoggerCQ.LogWarning($"QueryAsyncDownload file not found: Key={key}, File=\"{fileName}\"");
                        _asyncQueryRunning.TryRemove(key, out QueryThreaded z);
                        return null;
                    }

                    const int chunkSize = 1024 * 1024 * 4;
                    var offset = (chunk * chunkSize);
                    byte[] buffer;
                    int bytes;
                    try
                    {
                        using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                        {
                            fs.Seek(offset, SeekOrigin.Begin);// this is relevent during a retry. otherwise, it just seeks to the start
                            buffer = new byte[chunkSize];
                            bytes = fs.Read(buffer, 0, chunkSize); // read the first chunk in the buffer (which is re-used for every chunk)
                        }

                        if (bytes != chunkSize)
                        {
                            // the last chunk will almost certainly not fill the buffer, so it must be trimmed before returning
                            byte[] trimmedBuffer = new byte[bytes];
                            Array.Copy(buffer, trimmedBuffer, bytes);
                            buffer = trimmedBuffer;
                        }
                    }
                    catch (Exception ex)
                    {
                        LoggerCQ.LogError(ex, $"QueryAsyncDownload file read failed: Key={key}, Chunk={chunk}, File=\"{fileName}\"");
                        _asyncQueryRunning.TryRemove(key, out QueryThreaded z);
                        return null;
                    }

                    //On last chunk delete file
                    if (bytes != chunkSize)
                    {
                        File.Delete(fileName);
                        LoggerCQ.LogDebug($"QueryAsyncDownload Complete: Key={key}, Chunk={chunk}, Datasize={buffer.Length}, Query=\"{queryString}\"");
                        _asyncQueryRunning.TryRemove(key, out QueryThreaded z);
                    }
                    else
                    {
                        LoggerCQ.LogDebug($"QueryAsyncDownload Processing: Key={key}, Chunk={chunk}, Datasize={buffer.Length}, Query=\"{queryString}\"");
                    }

                    return buffer;
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"QueryAsyncDownload failed: Key={key}, Chunk={chunk}, Query={queryString}");
                throw new FaultException(ex.Message);
            }
        }

        public int GetDataVersion(Guid repositoryId)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            LoggerCQ.LogTrace($"GetDataVersion begin: RepositoryId={repositoryId}");

            try
            {
                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning($"Repository not found: {repositoryId}");
                    //retval.ErrorList = new string[] { "The repository has not been initialized! ID: " + repositoryId };
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        return GetRepositoryChangeStamp(context, schema.InternalID);
                    }
                }
            }
            catch (RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"GetDataVersion failed: RepositoryId={repositoryId}");
                return 0;
            }
        }

        public bool ResetDimensionValue(Guid repositoryId, long dvidx, string value)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            try
            {
                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning($"Repository not found: {repositoryId}");
                    //retval.ErrorList = new string[] { "The repository has not been initialized! ID: " + repositoryId };
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        var dimensionList = _dimensionCache.Get(context, schema, schema.InternalID, new List<DataItem>());
                        var retval = SqlHelper.UpdateDimensionValue(schema, dimensionList, dvidx, value);
                        QueryCache.Clear(schema.InternalID, schema.ID);
                        _dimensionCache.Clear(schema.InternalID);
                        return retval;
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"ResetDimensionValue failed: RepositoryId={repositoryId}, dvIdx={dvidx}, Value=\"{value}\"");
                return false;
            }
        }

        public bool DeleteDimensionValue(Guid repositoryId, long dvidx)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            try
            {
                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning($"Repository not found: {repositoryId}");
                    //retval.ErrorList = new string[] { "The repository has not been initialized! ID: " + repositoryId };
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        var dimensionList = _dimensionCache.Get(context, schema, schema.InternalID, new List<DataItem>());
                        var d = dimensionList.FirstOrDefault(x => x.RefinementList.Any(z => z.DVIdx == dvidx));
                        if (d != null)
                        {
                            var retval = SqlHelper.DeleteDimensionValue(schema, dimensionList, dvidx);
                            d.RefinementList.RemoveAll(x => x.DVIdx == dvidx);
                            QueryCache.Clear(schema.InternalID, schema.ID);
                            _dimensionCache.Clear(schema.InternalID);
                            return retval;
                        }
                    }
                    return false;
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"DeleteDimensionValue failed: RepositoryId={repositoryId}, dvIdx={dvidx}");
                return false;
            }
        }

        private static void SchemaMismatchDebug(RepositorySchema client, RepositorySchema current)
        {
            if (client == null || current == null) return;
            try
            {
                var sb = new StringBuilder();
                sb.AppendLine("Client:" + client.FieldList.Select(x => x.Name).ToCommaList() +
                    "|Current:" + current.FieldList.Select(x => x.Name).ToCommaList());
                LoggerCQ.LogError("Non-matching schema\r\n" + sb.ToString());
            }
            catch (Exception ex)
            {
                //Do Nothing
            }
        }

        public SummarySliceValue CalculateSlice(Guid repositoryId, SummarySlice slice)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            var retval = new SummarySliceValue();
            try
            {
                var timer = Stopwatch.StartNew();

                if (slice.Query == null) throw new Exception("Query is required");
                if (slice.GroupFields == null) throw new Exception("GroupFields is required");
                //if (slice.Query.FieldSorts == null || slice.Query.FieldSorts.Count == 0) throw new Exception("FieldSorts is required");
                if (slice.Query.FieldSorts == null) slice.Query.FieldSorts = new List<IFieldSort>();

                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning($"Repository not found: {repositoryId}");
                    //retval.ErrorList = new string[] { "The repository has not been initialized! ID: " + repositoryId };
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        using (var q = new AcquireReaderLock(repositoryId, "Query"))
                        {
                            var id = schema.InternalID;
                            retval = QueryCache.GetSlice(context, slice, id);
                            var cacheHit = (retval != null);
                            if (!cacheHit)
                            {
                                var dimensionList = _dimensionCache.Get(context, schema, id, new List<DataItem>());

                                retval = SqlHelper.CalculateSlice(schema, id, slice, dimensionList, ConfigHelper.ConnectionString);
                                retval.DataVersion = GetRepositoryChangeStamp(context, schema.InternalID);
                                QueryCache.SetSlice(context, slice, id, retval);
                                timer.Stop();
                            }
                            LoggerCQ.LogDebug($"CalculateSlice: ID={repositoryId}" +
                                $", Cache=" + (cacheHit ? "1" : "0") +
                                $", Elapsed={timer.ElapsedMilliseconds}" +
                                $", LockTime={q.LockTime}" +
                                $", WorkTime={CalcWorkTime(timer, q.LockTime)}" +
                                $", PO={slice.Query.PageOffset}" +
                                $", RPP={slice.Query.RecordsPerPage}" +
                                $", Count={retval.RecordList.Count}" +
                                $", QueryString=\"{slice.Query.ToString()}\"");
                        }
                    }
                    return retval;
                }
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"CalculateSlice failed: RepositoryId={repositoryId}, Query=\"{slice.Query.ToString()}\"");
                throw new FaultException(ex.Message);
            }            
        }

        public void AddPermission(Guid repositoryId, IEnumerable<PermissionItem> list)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            if (list == null) return;
            if (list.Count() == 0) return;
            try
            {
                //Do not call RepositoryExists since this does the same thing
                var timer = Stopwatch.StartNew();
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning("Repository not found: ID=" + repositoryId);
                    //retval.ErrorList = new string[] { "The repository has not been initialized! ID: " + repositoryId };
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    var lockTime = 0;
                    using (var q = new AcquireWriterLock(ServerUtilities.RandomizeGuid(repositoryId, RSeedPermissions), "AddPermission"))
                    {
                        lockTime = q.LockTime;
                        //Clear the permissions of those that are suppposed to be reset
                        list.Where(x => x.Reset)
                            .Select(x => x.FieldValue)
                            .Distinct()
                            .ToList()
                            .ForEach(x => SqlHelper.ClearPermissions(schema, x));

                        SqlHelper.AddPermission(schema, list);
                        QueryCache.Clear(schema.InternalID, schema.ID);
                    }
                    timer.Stop();
                }
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"AddPermission failed: RepositoryId={repositoryId}");
                throw new FaultException(ex.Message);
            }
        }

        public void DeletePermission(Guid repositoryId, IEnumerable<PermissionItem> list)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            try
            {
                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning($"Repository not found: {repositoryId}");
                    //retval.ErrorList = new string[] { "The repository has not been initialized! ID: " + repositoryId };
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    using (var q = new AcquireWriterLock(ServerUtilities.RandomizeGuid(repositoryId, RSeedPermissions), "DeletePermission"))
                    {
                        SqlHelper.DeletePermission(schema, list);
                        QueryCache.Clear(schema.InternalID, schema.ID);
                    }
                }
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"DeletePermission failed: RepositoryId={repositoryId}");
                throw new FaultException(ex.Message);
            }
        }

        public void ClearPermissions(Guid repositoryId, string fieldValue)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            try
            {
                //Do not call RepositoryExists since this does the same thing
                var timer = Stopwatch.StartNew();
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning($"Repository not found: {repositoryId}");
                    //retval.ErrorList = new string[] { "The repository has not been initialized! ID: " + repositoryId };
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    var lockTime = 0;
                    if (SqlHelper.NeedClearPermissions(schema.ID))
                    {
                        using (var q = new AcquireWriterLock(ServerUtilities.RandomizeGuid(repositoryId, RSeedPermissions), "ClearPermissions"))
                        {
                            lockTime = q.LockTime;
                            SqlHelper.ClearPermissions(schema, fieldValue);
                            QueryCache.Clear(schema.InternalID, schema.ID);
                        }
                    }
                    timer.Stop();
                    LoggerCQ.LogDebug($"ClearPermissions: Elapsed={timer.ElapsedMilliseconds}" +
                        $", LockTime={lockTime}" +
                        $", WorkTime={CalcWorkTime(timer, lockTime)}" +
                        $", FieldValue={fieldValue}");
                }
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"ClearPermissions failed: RepositoryId={repositoryId}, FieldValue=\"{fieldValue}\"");
                throw new FaultException(ex.Message);
            }
        }

        public void ClearUserPermissions(Guid repositoryId, int userId)
        {
            if (!IsServerMaster())
                throw new NotMasterInstanceException();

            try
            {
                //Do not call RepositoryExists since this does the same thing
                var timer = Stopwatch.StartNew();
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning($"Repository not found: {repositoryId}");
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    var lockTime = 0;
                    if (SqlHelper.NeedClearPermissions(schema.ID))
                    {
                        using (var q = new AcquireWriterLock(ServerUtilities.RandomizeGuid(repositoryId, RSeedPermissions), "ClearPermissions"))
                        {
                            lockTime = q.LockTime;
                            SqlHelper.ClearUserPermissions(schema, userId);
                            QueryCache.Clear(schema.InternalID, schema.ID);
                        }
                    }
                    timer.Stop();
                }
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"ClearUserPermissions failed: RepositoryId={repositoryId}, UserId={userId}");
                throw new FaultException(ex.Message);
            }
        }

        /// <summary>
        /// Just returns a true if the service is running
        /// </summary>
        /// <remarks>The return is the instance value of this service</remarks>
        public bool IsServerAlive()
        {
            return _masterReset;
        }

        /// <summary>
        /// Just returns a true if the service is running
        /// </summary>
        /// <remarks>The return is the instance value of this service</remarks>
        public bool IsServerMaster()
        {
            return IsMaster();
        }

        private static bool IsMaster()
        {
            return true; //InstanceId == ConfigHelper.CurrentMaster;
        }

        /// <summary>
        /// This will clear all cache objects and the service will be reset to the initial loaded state
        /// </summary>
        public bool ResetMaster()
        {
            try
            {
                //if (IsServerMaster())
                //{
                //    //Do Nothing
                //}
                //else
                //{
                //    //Try to promote to master
                //    if (!ConfigHelper.PromoteMaster())
                //        return false;
                //}

                _dimensionCache = new DimensionCache();
                QueryCache.Reset();
                _dimensionChangeStampCache = new Dictionary<int, int>();
                _repositoryChangeStampCache = new Dictionary<Guid, int>();
                _repositoryExistCache = new ConcurrentHashSet<Guid>();
                _schemaCache.Reset();
                _statsMaintenance = new TableStatsMaintenace(_system.EnableHouseKeeping);
                _housekeepingMonitor = new HousekeepingMonitor();
                SqlHelper.Reset();
                LoggerCQ.LogInfo("System Reset");
                return true;
            }
            finally
            {
                _masterReset = true;
            }
        }

        internal static List<Guid> GetChildRepositories(Guid repositoryId)
        {
            var retval = new List<Guid>();
            try
            {
                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                {
                    var id = context.Repository
                        .Where(x => x.UniqueKey == repositoryId)
                        .Select(x => x.RepositoryId)
                        .FirstOrDefault();
                    retval.AddRange(context.Repository.Where(x => x.ParentId == id).Select(x => x.UniqueKey).ToList());
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
            return retval;
        }

        /// <summary>
        /// This will calculate the complete list of dimensions and append it to the results object's AllDimensionList
        /// so that the client has a master list of all dimensions
        /// </summary>
        private List<DimensionItem> GetAllDimensions(DataQuery query, int id, DatastoreEntities context, RepositorySchema schema, List<DimensionItem> dimensionList)
        {
            var newQuery = new DataQuery();
            newQuery.IncludeRecords = false;
            newQuery.IncludeEmptyDimensions = true;
            newQuery.SkipDimensions = query.SkipDimensions;
            newQuery.NonParsedFieldList = query.NonParsedFieldList;

            bool isCore;
            var retval = QueryCache.Get(context, newQuery, id, schema.ID, out isCore);
            if (retval == null)
            {
                retval = SqlHelper.Query(schema, id, newQuery, dimensionList);
                if (newQuery.SkipDimensions.Any())
                    retval.DimensionList.RemoveAll(x => newQuery.SkipDimensions.Contains(x.DIdx));
                QueryCache.Set(context, newQuery, id, schema.ID, retval);
            }
            return retval.DimensionList;
        }

        internal int CacheCount
        {
            get { return QueryCache.Count; }
        }

        RepositorySchema IDataModel.GetSchema(Guid repositoryId)
        {
            return GetSchema(repositoryId);
        }

        bool IDataModel.RepositoryExists(Guid repositoryId)
        {
            return RepositoryExists(repositoryId);
        }

        private class UpdateDataWhereCacheItem
        {
            public RepositorySchema schema { get; set; }
            public DataQuery query { get; set; }
            public IEnumerable<DataFieldUpdate> list { get; set; }
        }

        /// <summary>
        /// Sometimes the lock time > elapsed so just normalize it so that it does not look odd in log
        /// </summary>
        private long CalcWorkTime(Stopwatch timer, int lockTime)
        {
            var v = timer.ElapsedMilliseconds - lockTime;
            if (v < 0) v = 0;
            return v;
        }
    }
}