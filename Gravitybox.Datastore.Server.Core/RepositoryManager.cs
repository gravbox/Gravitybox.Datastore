#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.Server.Interfaces;
using System.IO;
using System.Threading.Tasks;
using System.Diagnostics;
using Gravitybox.Datastore.EFDAL;
using System.Collections.Concurrent;
using Gravitybox.Datastore.Server.Core.Housekeeping;
using Gravitybox.Datastore.Common.Queryable;

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
        private QueryCache _queryCache = new QueryCache();
        public static Dictionary<int, int> _dimensionChangeStampCache = new Dictionary<int, int>();
        private static Dictionary<Guid, int> _repositoryChangeStampCache = new Dictionary<Guid, int>();
        private static Dictionary<Guid, RepositoryCacheItem> _schemaCache = new Dictionary<Guid, RepositoryCacheItem>();
        private static Dictionary<Guid, long> _schemaVersionCache = new Dictionary<Guid, long>();
        private static ConcurrentDictionary<int, int?> _schemaParentCache = new ConcurrentDictionary<int, int?>();
        private static ConcurrentHashSet<Guid> _repositoryExistCache = new ConcurrentHashSet<Guid>();
        private QueryLogManager _queryLogManager = new QueryLogManager();
        private static readonly Guid RepositoryExistsID = new Guid("6541FFC1-F4F6-477E-B9AC-B10E59C4BD63");
        private static readonly Guid SchemaCacheID = new Guid("1726FFC1-F4F6-477E-B9AC-B10E59C4BD63");
        private static readonly Guid DimensionCacheID = new Guid("2726FFC1-F4F6-477E-B9AC-B10E59C4BD63");
        private static readonly Guid RepositoryChangeStampID = new Guid("9941FFC1-F4F6-477E-B9AC-B10E59C4BD63");
        private static readonly Guid AllRepositoryLockID = new Guid("D517782E-F4F6-477E-B9AC-B10E59C4BD63");
        private static TableStatsMaintenace _statsMaintenance = new TableStatsMaintenace();
        private static HousekeepingMonitor _housekeepingMonitor = new HousekeepingMonitor();
        private const byte RSeedPermissions = 187;

        public RepositoryManager(ISystemCore system)
        {
            try
            {
                //DatastoreLock.RegisterMachine();
                _system = system;
                SqlHelper.Initialize(_queryCache);
                _fileGroups = SqlHelper.GetFileGroups(ConfigHelper.ConnectionString);
                LoggerCQ.LogInfo("Filegroups: Count=" + _fileGroups.Count);

                //This will process the Async update where statements
                _timerUpdateDataWhereAsync = new System.Timers.Timer(10000);
                _timerUpdateDataWhereAsync.Elapsed += _timerUpdateDataWhereAsync_Elapsed;
                _timerUpdateDataWhereAsync.Start();

                Task.Factory.StartNew(() => { this.SetupParentIdCache(); });

            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public static ISystemCore SystemCore
        {
            get { return _system; }
        }

        public static void SetRepositoryChangeStamp(Guid id)
        {
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
                throw;
            }
        }

        public static int GetRepositoryChangeStamp(DatastoreEntities context, int id)
        {
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
                throw;
            }
        }

        public static void SetDimensionChanged(int id)
        {
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
                throw;
            }
        }

        public static int GetDimensionChanged(DatastoreEntities context, int id)
        {
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
                throw;
            }
        }

        public void RemoveRepository(Guid repositoryId)
        {
            try
            {
                var timer = Stopwatch.StartNew();
                timer.Start();

                //Delete from list in case error
                var id = 0;
                RepositorySchema schema;
                using (var q = new AcquireWriterLock(repositoryId, "RemoveRepository"))
                {
                    if (!RepositoryExists(repositoryId)) return;
                    schema = GetSchema(repositoryId);
                    id = schema.InternalID;
                    LoggerCQ.LogDebug("Starting RemoveRepository: ID=" + repositoryId + ", InternalId=" + id);

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
                    _queryCache.Clear(schema.InternalID, schema.ID);
                    ClearSchemaCache(schema.ID);
                }

                timer.Stop();
                LoggerCQ.LogDebug("RemoveRepository: ID=" + repositoryId + ", Elapsed=" + timer.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                throw;
            }
        }

        public void AddRepository(RepositorySchema schema)
        {
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

                        parentSchema = GetSchema(schema.ParentID.Value);
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
                    LoggerCQ.LogDebug("AddRepository: ID=" + schema.ID + ", Elapsed=" + timer.ElapsedMilliseconds);

                    var schema2 = GetSchema(schema.ID);
                    if (schema2 != null)
                    {
                        _queryCache.Clear(schema2.InternalID, schema2.ID);
                        RepositoryCacheAdd(schema2.ID);
                    }
                }

            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={schema.ID}");
                throw;
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
            if (schema == null) throw new Exception("Schema was not specified.");
            var retval = new ActionDiagnostics { RepositoryId = schema.ID, IsSuccess = false };
            var errorList = new List<string>();
            try
            {
                var schema1 = GetSchema(schema.ID);
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
                            _queryCache.Clear(schema.InternalID, schema.ID);

                        timer.Stop();
                        retval.ComputeTime = timer.ElapsedMilliseconds;
                        lockTime = q.LockTime;
                        retval.LockTime = q.LockTime;
                        retval.IsSuccess = true;
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

                    LoggerCQ.LogDebug("DeleteItems: ID=" + schema.ID +
                        ", Elapsed=" + timer.ElapsedMilliseconds +
                        ", LockTime=" + lockTime +
                        ", WorkTime=" + (timer.ElapsedMilliseconds - lockTime) +
                        (waitingLocks > 0 ? ", WaitLocks=" + waitingLocks : string.Empty) +
                        ", Cached=" + (itemCount - count) +
                        ", Count=" + itemCount);
                }
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={schema.ID}");
                errorList.Add(ex.ToString());
            }
            retval.Errors = errorList.ToArray();
            return retval;
        }

        public ActionDiagnostics DeleteData(RepositorySchema schema, DataQuery query)
        {
            var retval = new ActionDiagnostics { RepositoryId = schema.ID, IsSuccess = false };
            var errorList = new List<string>();
            var timer = Stopwatch.StartNew();
            try
            {
                //TODO: Verify Credentials
                var lockTime = 0;
                var schema1 = GetSchema(schema.ID);
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
                                _queryCache.Clear(schema.InternalID, schema.ID);
                        }
                        timer.Stop();
                        retval.ComputeTime = timer.ElapsedMilliseconds;
                        retval.LockTime = q.LockTime;
                        retval.IsSuccess = true;
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
                        ", Elapsed=" + timer.ElapsedMilliseconds +
                        ", LockTime=" + lockTime +
                        ", WorkTime=" + (timer.ElapsedMilliseconds - lockTime) +
                        (waitingLocks > 0 ? ", WaitLocks=" + waitingLocks : string.Empty) +
                        ", Count=" + count +
                        ", QueryString=\"" + query.ToString() + "\"");

                }
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={schema.ID}");
                errorList.Add(ex.ToString());
            }
            finally
            {
                //If takes too long then mark for statistics refresh
                if (timer.ElapsedMilliseconds > TableStatsMaintenace.StatCheckThreshold)
                    _statsMaintenance.MarkRefreshStats(schema.ID);
            }
            retval.Errors = errorList.ToArray();
            return retval;
        }

        public ActionDiagnostics Clear(Guid repositoryId)
        {
            var retval = new ActionDiagnostics { RepositoryId = repositoryId, IsSuccess = false };
            var errorList = new List<string>();
            var timer = Stopwatch.StartNew();
            try
            {
                if (!RepositoryExists(repositoryId))
                {
                    LoggerCQ.LogWarning("Repository not found: " + repositoryId);
                    //errorList.Add("The repository has not been initialized! ID: " + repositoryId);
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    LoggerCQ.LogDebug("Clear: ID=" + repositoryId);
                    using (var q = new AcquireWriterLock(repositoryId, "Clear"))
                    {
                        var schema = GetSchema(repositoryId);
                        SqlHelper.Clear(schema, ConfigHelper.ConnectionString);
                        _queryCache.Clear(schema.InternalID, schema.ID);
                        retval.LockTime = q.LockTime;
                        retval.IsSuccess = true;
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
                errorList.Add(ex.ToString());
            }

            timer.Stop();
            retval.ComputeTime = timer.ElapsedMilliseconds;
            retval.Errors = errorList.ToArray();
            return retval;
        }

        private static ConcurrentDictionary<string, DateTime> _runningQueries = new ConcurrentDictionary<string, DateTime>();

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
            DataQueryResults retval = null;
            if (query == null) return null;
            var queryString = query.ToString();
            var queryKey = queryString + "|" + repositoryId;

            var lockTime = 0;
            var timer = Stopwatch.StartNew();
            try
            {
                //If the exact same query is running then wait until it is cached and then process this one
                var didWait = false;
                var waitTime = 0;
                while (!_runningQueries.TryAdd(queryKey, DateTime.Now))
                {
                    didWait = true;
                    System.Threading.Thread.Sleep(30);
                }

                //Log if there was a wait.
                if (didWait)
                {
                    waitTime = (int)timer.ElapsedMilliseconds;
                    LoggerCQ.LogInfo($"Query Wait, ID={repositoryId}, Elapsed={timer.ElapsedMilliseconds}, WaitCount={_runningQueries.Count}");
                }

                //TODO: Verify Credentials

                var waitingLocks = 0;
                var readLockCount = 0;

                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning("Repository not found: " + repositoryId);
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
                            retval = _queryCache.Get(context, query, id, schema.ID, out isCore);
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
                                if (query.IncludeDimensions)
                                {
                                    allDimTask = Task.Factory.StartNew(() =>
                                    {
                                        allDimensions = GetAllDimensions(query.SkipDimensions, id, context, schema, dimensionList);
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
                                                    _queryCache.Set(context, q2, id, schema.ID, newR);
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
                                        _queryCache.Set(context, query, id, schema.ID, retval);
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
                            ", WorkTime=" + (timer.ElapsedMilliseconds - lockTime) +
                            (waitingLocks > 0 ? ", WaitLocks=" + waitingLocks : string.Empty) +
                            ", PO=" + query.PageOffset + ", RPP=" + query.RecordsPerPage +
                            ", Count=" + retval.RecordList.Count +
                            ", Total=" + retval.TotalRecordCount +
                            (didWait ? ", DidWait=1, WaitTime="+ waitTime : string.Empty) +
                            (recordMultiplier > 0 ? ", RecordMultiplier=" + recordMultiplier : "") +
                            (string.IsNullOrEmpty(executeHistory) ? string.Empty : ", EH=" + executeHistory);
                            logMsg += ", QueryString=\"" + queryString + "\"";
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
                        LockTime = lockTime,
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
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"Query: RepositoryId={repositoryId}, QueryString=\"{query.ToString()}\", Elapsed={timer.ElapsedMilliseconds}");
                retval.ErrorList = new string[] { ex.ToString() };
            }
            finally
            {
                DateTime d;
                var b = _runningQueries.TryRemove(queryKey, out d);
                if (!b) LoggerCQ.LogDebug("Running query dequeue failed");

                //If takes too long then mark for statistics refresh
                if (timer.ElapsedMilliseconds > TableStatsMaintenace.StatCheckThreshold)
                    _statsMaintenance.MarkRefreshStats(repositoryId);
            }
            return retval;
        }

        public int GetLastTimestamp(Guid repositoryId, DataQuery query)
        {
            try
            {
                var timer = Stopwatch.StartNew();

                //TODO: Verify Credentials

                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning("Repository not found: " + repositoryId);
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
            }
            return 0;
        }

        public int GetTimestamp()
        {
            return Utilities.CurrentTimestamp;
        }

        public void ShutDown()
        {
            _queryLogManager.Empty();
        }

        public void ShutDown(Guid repositoryId)
        {
            try
            {
                if (!RepositoryExists(repositoryId))
                {
                    LoggerCQ.LogWarning("Repository not found: " + repositoryId);
                    //throw new Exception("The repository has not been initialized! ID: " + repositoryId);
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }

                //repository.ServiceInstance.ShutDown();
                //repository = null;

            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                throw;
            }
        }

        /// <summary>
        /// Returns the number of items in the repository
        /// </summary>
        /// <returns></returns>
        public long GetItemCount(Guid repositoryId)
        {
            try
            {
                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                {
                    using (var q = new AcquireReaderLock(repositoryId, "GetItemCount"))
                    {
                        var schema = GetSchema(repositoryId);
                        if (schema == null) return 0;
                        return SqlHelper.Count(schema, schema.InternalID, ConfigHelper.ConnectionString);
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                throw;
            }
        }

        internal static void ClearSchemaCache(Guid repositoryId)
        {
            try
            {
                //Clear the schema cache
                using (var l3 = new AcquireWriterLock(SchemaCacheID, "SchemaCache"))
                {
                    var id = 0;
                    if (_schemaCache.ContainsKey(repositoryId))
                    {
                        var schema = RepositorySchema.CreateFromXml(_schemaCache[repositoryId].Xml);
                        id = schema.InternalID;
                        _schemaCache.Remove(repositoryId);
                    }
                    if (_schemaVersionCache.ContainsKey(repositoryId))
                        _schemaVersionCache.Remove(repositoryId);

                    int? v;
                    _schemaParentCache.TryRemove(id, out v);
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        internal static long GetSchemaHash(Guid repositoryId, bool clear = false)
        {
            try
            {
                if (clear) ClearSchemaCache(repositoryId);
                using (var l3 = new AcquireWriterLock(SchemaCacheID, "SchemaCache"))
                {
                    if (_schemaVersionCache.ContainsKey(repositoryId))
                        return _schemaVersionCache[repositoryId];

                    var schema = GetSchema(repositoryId);
                    if (schema != null) return schema.VersionHash;
                    return 0;
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        internal static int? GetSchemaParentId(int repositoryId)
        {
            try
            {
                return _schemaParentCache.GetOrAdd(repositoryId, (q) =>
                {
                    using (var context = new DatastoreEntities())
                    {
                        return context.Repository
                                    .Where(x => x.RepositoryId == repositoryId)
                                    .Select(x => x.ParentId)
                                    .FirstOrDefault();
                    }
                });
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        private void SetupParentIdCache()
        {
            try
            {
                using (var context = new DatastoreEntities())
                {
                    var list = context.Repository
                                .Select(x => new { x.RepositoryId, x.ParentId })
                                .ToList();

                    foreach (var item in list)
                    {
                        _schemaParentCache.TryAdd(item.RepositoryId, item.ParentId);
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        internal static RepositorySchema GetSchema(Guid repositoryId)
        {
            try
            {
                if (!RepositoryExists(repositoryId))
                {
                    throw new Exception($"The repository does not exist. RepositoryId={repositoryId}");
                }
                else
                {
                    return GetSchema(repositoryId, false);
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        internal static RepositorySchema GetSchema(Guid repositoryId, bool clear)
        {
            const int MaxTry = 4;
            var tryCount = 0;
            do
            {
                //LoggerCQ.LogDebug("GetSchema: Start, ID=" + repositoryId);
                try
                {
                    if (clear) ClearSchemaCache(repositoryId);
                    using (var l3 = new AcquireWriterLock(SchemaCacheID, "SchemaCache"))
                    {
                        if (!_schemaCache.ContainsKey(repositoryId))
                        {
                            using (var context = new DatastoreEntities())
                            {
                                var r = context.Repository.FirstOrDefault(x => x.UniqueKey == repositoryId && !x.IsDeleted && x.IsInitialized);
                                if (r == null) return null;
                                var retval = new RepositorySchema();
                                retval.LoadXml(r.DefinitionData);
                                retval.InternalID = r.RepositoryId;
                                retval.ChangeStamp = r.Changestamp;

                                #region Find parent repository
                                RepositorySchema parentRepository = null;
                                if (r.ParentId != null)
                                {
                                    var pr = context.Repository.FirstOrDefault(x => x.RepositoryId == r.ParentId);
                                    if (pr != null)
                                    {
                                        parentRepository = new RepositorySchema();
                                        parentRepository.LoadXml(pr.DefinitionData);
                                        parentRepository.InternalID = pr.RepositoryId;
                                        parentRepository.ChangeStamp = pr.Changestamp;
                                    }
                                }
                                #endregion

#if DEBUG
                                //Ensure the key does not exist (used in debugging)
                                if (_schemaCache.ContainsKey(repositoryId))
                                    _schemaCache.Remove(repositoryId);
#endif

                                _schemaCache.Add(repositoryId, new RepositoryCacheItem
                                {
                                    Xml = retval.ToXml(true),
                                    HasParent = (r.ParentId != null),
                                });

                                //Cache the version to reduce number of calculations
                                if (_schemaVersionCache.ContainsKey(repositoryId))
                                    _schemaVersionCache.Remove(repositoryId);
                                _schemaVersionCache.Add(repositoryId, retval.VersionHash);
                            }
                        }

                        return RepositorySchema.CreateFromXml(_schemaCache[repositoryId].Xml);

                    }
                }
                catch (System.Data.SqlClient.SqlException ex)
                {
                    if (ex.Message.ToLower().Contains("deadlock"))
                    {
                        LoggerCQ.LogWarning($"GetSchema deadlock {tryCount}");
                        tryCount++;
                    }
                    else
                    {
                        LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                        throw;
                    }
                    System.Threading.Thread.Sleep(_rnd.Next(150, 500));
                }
                catch (Exception ex)
                {
                    LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                    throw;
                }
            } while (tryCount < MaxTry);
            throw new Exception("Cannot complete operation.");
        }

        public bool IsValidFormat(Guid repositoryId, DataItem item)
        {
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
                        if (item.ItemArray == null) return false;
                        if (item.ItemArray.Length != schema.FieldList.Count) return false;

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
                                    case RepositorySchema.DataTypeConstants.String:
                                        if (!(item.ItemArray[index] is string)) return false;
                                        break;
                                    //case RepositorySchema.DataTypeConstants.List:
                                    //    if (!(item.ItemArray[index] is string[])) return false;
                                    //    break;
                                    default:
                                        LoggerCQ.LogWarning("IsItemValid: Unknown data type: " + field.DataType.ToString());
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
                LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                throw;
            }
        }

        internal static bool RepositoryExists(Guid repositoryId)
        {
            const int MaxTry = 4;
            var tryCount = 0;
            var timer = Stopwatch.StartNew();
            do
            {
                //var cacheHit = false;
                var theValue = false;
                try
                {
                    //Cache whether a repository exists
                    theValue = _repositoryExistCache.Contains(repositoryId);
                    if (theValue)
                    {
                        //cacheHit = true;
                        return theValue;
                    }

                    //If made it here then need to hit database
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        //using (var l3 = new AcquireReaderLock(repositoryId, "RepositoryExists2"))
                        {
                            theValue = context.Repository.Any(x => x.UniqueKey == repositoryId && !x.IsDeleted && x.IsInitialized);
                            if (theValue) _repositoryExistCache.Add(repositoryId);
                            else _repositoryExistCache.Remove(repositoryId);
                            return theValue;
                        }
                    }

                }
                catch (System.Data.SqlClient.SqlException ex)
                {
                    if (ex.Message.ToLower().Contains("deadlock"))
                    {
                        LoggerCQ.LogWarning("RepositoryExists deadlock " + tryCount);
                        tryCount++;
                    }
                    else
                    {
                        LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                        throw;
                    }
                    System.Threading.Thread.Sleep(_rnd.Next(150, 500));
                }
                catch (Exception ex)
                {
                    LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                    throw;
                }
                finally
                {
                    timer.Stop();
                    //LoggerCQ.LogDebug("RepositoryExists: ID=" + repositoryId + ", CacheHit=" + cacheHit + ", Value=" + theValue + ", Elapsed=" + timer.ElapsedMilliseconds);
                }
            } while (tryCount < MaxTry);
            throw new Exception("Cannot complete operation.");
        }

        private void RepositoryCacheRemove(Guid repositoryId)
        {
            try
            {
                try
                {
                    _repositoryExistCache.Remove(repositoryId);
                }
                catch (Exception ex)
                {
                    LoggerCQ.LogError(ex);
                    throw;
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        private void RepositoryCacheAdd(Guid repositoryId)
        {
            try
            {
                _repositoryExistCache.Add(repositoryId);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public ActionDiagnostics UpdateData(RepositorySchema schema, IEnumerable<DataItem> list)
        {
            var retval = new ActionDiagnostics { RepositoryId = schema.ID, IsSuccess = false };
            if (list == null)
            {
                retval.IsSuccess = true;
                return retval;
            }

            var errorList = new List<string>();
            var timer = Stopwatch.StartNew();
            try
            {
                using (var q = new AcquireWriterLock(schema.ID, TraceInfoUpdateData))
                {
                    var schema1 = GetSchema(schema.ID);
                    if (schema1 == null)
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
                            LoggerCQ.LogDebug("UpdateData: ID=" + schema.ID +
                                ", Elapsed=" + timer.ElapsedMilliseconds +
                                ", LockTime=" + q.LockTime +
                                ", WorkTime=" + (timer.ElapsedMilliseconds - q.LockTime) +
                                (q.WaitingLocksOnEntry > 0 ? ", WaitLocks=" + q.WaitingLocksOnEntry : string.Empty) +
                                ", Found=" + results.FountCount +
                                ", Cached=" + (itemCount - results.AffectedCount) +
                                ", Count=" + itemCount);

                            if (results.AffectedCount > 0)
                                _queryCache.Clear(id, schema.ID);

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
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={schema.ID}, Elapsed={timer.ElapsedMilliseconds}");
                errorList.Add(ex.ToString());
            }
            finally
            {
                //If takes too long then mark for statistics refresh
                if (timer.ElapsedMilliseconds > TableStatsMaintenace.StatCheckThreshold)
                    _statsMaintenance.MarkRefreshStats(schema.ID);
            }
            retval.Errors = errorList.ToArray();
            return retval;
        }

        #region UpdateDataWhereAsync
        private System.Timers.Timer _timerUpdateDataWhereAsync = null;
        private System.Collections.Concurrent.ConcurrentQueue<UpdateDataWhereCacheItem> _updateDataWhereQueue = new System.Collections.Concurrent.ConcurrentQueue<UpdateDataWhereCacheItem>();
        public void UpdateDataWhereAsync(RepositorySchema schema, DataQuery query, IEnumerable<DataFieldUpdate> list)
        {
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
                LoggerCQ.LogError(ex);
                //throw;
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
                LoggerCQ.LogError(ex);
            }
            finally
            {
                _timerUpdateDataWhereAsync.Start();
            }
        }
        #endregion

        public ActionDiagnostics UpdateDataWhere(RepositorySchema schema, DataQuery query, IEnumerable<DataFieldUpdate> list)
        {
            var retval = new ActionDiagnostics { RepositoryId = schema.ID, IsSuccess = false };
            var errorList = new List<string>();
            try
            {
                using (var q = new AcquireReaderLock(schema.ID, TraceInfoUpdateData))
                {
                    var schema1 = GetSchema(schema.ID);
                    if (schema1 == null)
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
                        var timer = Stopwatch.StartNew();
                        timer.Start();

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
                            LoggerCQ.LogDebug("UpdateDataWhere: ID=" + schema.ID +
                                ", Elapsed=" + timer.ElapsedMilliseconds +
                                ", LockTime=" + q.LockTime +
                                ", WorkTime=" + (timer.ElapsedMilliseconds - q.LockTime) +
                                (q.WaitingLocksOnEntry > 0 ? ", WaitLocks=" + q.WaitingLocksOnEntry : string.Empty) +
                                ", Count=" + results.AffectedCount +
                                ", QueryString=\"" + query.ToString() + "\"");
                            _queryCache.Clear(id, schema.ID);

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
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={schema.ID}");
                errorList.Add(ex.ToString());
            }

            retval.Errors = errorList.ToArray();
            return retval;
        }

        public ActionDiagnostics UpdateSchema(RepositorySchema newSchema)
        {
            return UpdateSchema(newSchema, false);
        }

        internal ActionDiagnostics UpdateSchema(RepositorySchema newSchema, bool extremeVerify = false)
        {
            var retval = new ActionDiagnostics { IsSuccess = false };
            if (newSchema == null)
            {
                retval.Errors = new string[] { "The repository does not exist." };
                return retval;
            }
            retval.RepositoryId = newSchema.ID;

            var otherLockList = new List<AcquireWriterLock>();
            try
            {
                var timer = Stopwatch.StartNew();
                timer.Start();
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
                        retval.Errors = new string[] { $"The repository does not exist. RepositoryId={newSchema.ID}" };
                        return retval;
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
                                retval.Errors = new string[] { $"The repository does not exist. RepositoryId={newSchema.ID}" };
                                return retval;
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
                            actionResults = SqlHelper.UpdateSchema(ConfigHelper.ConnectionString, currentSchema, newSchema, extremeVerify);
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
                                var dimension = field as DimensionDefinition;
                                var useIndex = field.AllowIndex;
                                if (field.DataType == RepositorySchema.DataTypeConstants.String && ((field.Length <= 0) || (field.Length > 450)))
                                    useIndex = false;

                                if (useIndex)
                                {
                                    var indexName = SqlHelper.GetIndexName(field, dataTable);
                                    sbFields.AppendLine("if not exists(select * from sys.indexes where name = '" + indexName + "')");
                                    sbFields.AppendLine("CREATE NONCLUSTERED INDEX [" + indexName + "] ON [" + dataTable + "] ([" + field.Name + "] ASC);");
                                    sbFields.AppendLine();
                                }
                                else
                                {
                                    var indexName = SqlHelper.GetIndexName(field, dataTable);
                                    sbFields.AppendLine("if exists(select * from sys.indexes where name = '" + indexName + "')");
                                    sbFields.AppendLine("DROP INDEX [" + indexName + "] ON [" + dataTable + "]");
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
                            using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                            {
                                SqlHelper.UpdateFTSIndex(ConfigHelper.ConnectionString, newSchema);
                            }
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
                                        var c = context.SaveChanges();
                                        _queryCache.Clear(repositoryId, newSchema.ID);
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
                                    LoggerCQ.LogWarning($"UpdateSchema deadlock {tryCount}");
                                    tryCount++;
                                }
                                else
                                {
                                    LoggerCQ.LogError(ex);
                                    throw;
                                }
                                System.Threading.Thread.Sleep(_rnd.Next(150, 500));
                            }
                        } while (!isFinish && tryCount < maxTries);

                        _queryCache.Clear(repositoryId, newSchema.ID);
                        _dimensionCache.Clear(repositoryId);

                        timer.Stop();
                        retval.ComputeTime = timer.ElapsedMilliseconds;
                        retval.LockTime = q.LockTime;
                        retval.IsSuccess = true;
                        retval.Errors = actionResults.Errors.ToArray();

                        LoggerCQ.LogDebug($"UpdateSchema: ID={newSchema.ID}" +
                            $", Elapsed={timer.ElapsedMilliseconds}" +
                            $", LockTime={q.LockTime}" +
                            $", WorkTime={(timer.ElapsedMilliseconds - q.LockTime)}");
                        ClearSchemaCache(newSchema.ID);

                        return retval;
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                //Dispose of all other locked repositories
                otherLockList.ForEach(x => x.Dispose());
            }
        }

        public Guid QueryAsync(Guid repositoryId, DataQuery query)
        {
            try
            {
                //TODO: Verify Credentials

                if (!Directory.Exists(ConfigHelper.AsyncCachePath)) return Guid.Empty;

                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning("Repository not found: " + repositoryId);
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    var item = new QueryThreaded(_dimensionCache, schema, query);
                    var myThread = new System.Threading.Thread(new System.Threading.ThreadStart(item.Run));
                    myThread.Start();
                    LoggerCQ.LogDebug("QueryAsync: ID=" + repositoryId + ", Key=" + item.Key + ", QueryString=\"" + query.ToString() + "\"");
                    return item.Key;
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public bool QueryAsyncReady(Guid key)
        {
            try
            {
                //TODO: Verify Credentials
                //TODO: Verify that file is fully written!!!!!
                if (!Directory.Exists(ConfigHelper.AsyncCachePath)) return false;
                LoggerCQ.LogDebug("QueryAsyncReady: Key=" + key);
                var errorFileName = Path.Combine(ConfigHelper.AsyncCachePath, key.ToString() + ".error");
                if (File.Exists(errorFileName))
                    return true;

                var fileName = Path.Combine(ConfigHelper.AsyncCachePath, key.ToString() + ".zzz");
                return File.Exists(fileName);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public byte[] QueryAsyncDownload(Guid key, long chunk)
        {
            try
            {
                //TODO: Verify Credentials

                if (chunk < 0) return null;

                if (!Directory.Exists(ConfigHelper.AsyncCachePath)) return null;
                var fileName = Path.Combine(ConfigHelper.AsyncCachePath, key.ToString() + ".zzz");

                var errorFileName = Path.Combine(ConfigHelper.AsyncCachePath, key.ToString() + ".error");
                if (File.Exists(errorFileName))
                {
                    LoggerCQ.LogDebug("QueryAsyncDownload Error: Key=" + key + ", Chunk=" + chunk);
                    if (File.Exists(fileName)) File.Delete(fileName);
                    File.Delete(errorFileName);
                    return null;
                }
                else
                {
                    if (!File.Exists(fileName))
                    {
                        LoggerCQ.LogWarning("QueryAsyncDownload file not found: Key=" + key + ", File=" + fileName);
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
                        LoggerCQ.LogError(ex);
                        return null;
                    }

                    //On last chunk delete file
                    if (bytes != chunkSize)
                    {
                        File.Delete(fileName);
                        LoggerCQ.LogDebug("QueryAsyncDownload Complete: Key=" + key + ", Chunk=" + chunk + ", Datasize=" + buffer.Length);
                    }
                    else
                    {
                        LoggerCQ.LogDebug("QueryAsyncDownload Processing: Key=" + key + ", Chunk=" + chunk + ", Datasize=" + buffer.Length);
                    }

                    return buffer;
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public int GetDataVersion(Guid repositoryId)
        {
            try
            {
                //TODO: Verify Credentials

                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning("Repository not found: " + repositoryId);
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
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return 0;
            }
        }

        public bool ResetDimensionValue(Guid repositoryId, long dvidx, string value)
        {
            try
            {
                //TODO: Verify Credentials

                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning("Repository not found: " + repositoryId);
                    //retval.ErrorList = new string[] { "The repository has not been initialized! ID: " + repositoryId };
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        var dimensionList = _dimensionCache.Get(context, schema, schema.InternalID, new List<DataItem>());
                        var retval = SqlHelper.UpdateDimensionValue(schema, dimensionList, dvidx, value);
                        _queryCache.Clear(schema.InternalID, schema.ID);
                        _dimensionCache.Clear(schema.InternalID);
                        return retval;
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return false;
            }
        }

        public bool DeleteDimensionValue(Guid repositoryId, long dvidx)
        {
            try
            {
                //TODO: Verify Credentials

                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning("Repository not found: " + repositoryId);
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
                            _queryCache.Clear(schema.InternalID, schema.ID);
                            _dimensionCache.Clear(schema.InternalID);
                            return retval;
                        }
                    }
                    return false;
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return false;
            }
        }

        private static void SchemaMismatchDebug(RepositorySchema client, RepositorySchema current)
        {
            if (client == null || current == null) return;
            try
            {
                var sb = new StringBuilder();
                sb.AppendLine("Client:" + string.Join(",", client.FieldList.Select(x => x.Name)) +
                    "|Current:" + string.Join(",", current.FieldList.Select(x => x.Name)));
                LoggerCQ.LogError("Non-matching schema\r\n" + sb.ToString());
            }
            catch (Exception ex)
            {
                //Do Nothing
            }
        }

        public SummarySliceValue CalculateSlice(Guid repositoryId, SummarySlice slice)
        {
            
            var retval = new SummarySliceValue();
            try
            {
                var timer = Stopwatch.StartNew();

                if (slice.Query == null) throw new Exception("Query is required");
                if (slice.GroupFields == null) throw new Exception("GroupFields is required");
                //if (slice.Query.FieldSorts == null || slice.Query.FieldSorts.Count == 0) throw new Exception("FieldSorts is required");
                if (slice.Query.FieldSorts == null) slice.Query.FieldSorts = new List<IFieldSort>();

                //TODO: Verify Credentials

                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning("Repository not found: " + repositoryId);
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
                            retval = _queryCache.GetSlice(context, slice, id);
                            var cacheHit = (retval != null);
                            if (!cacheHit)
                            {
                                var dimensionList = _dimensionCache.Get(context, schema, id, new List<DataItem>());

                                retval = SqlHelper.CalculateSlice(schema, id, slice, dimensionList, ConfigHelper.ConnectionString);
                                retval.DataVersion = GetRepositoryChangeStamp(context, schema.InternalID);
                                _queryCache.SetSlice(context, slice, id, retval);
                                timer.Stop();
                            }
                            LoggerCQ.LogDebug("Slice: ID=" + repositoryId + 
                                ", Cache=" + (cacheHit ? "1" : "0") + 
                                ", Elapsed=" + timer.ElapsedMilliseconds +
                                ", LockTime=" + q.LockTime +
                                ", WorkTime=" + (timer.ElapsedMilliseconds - q.LockTime) +
                                ", PO=" + slice.Query.PageOffset +
                                ", RPP=" + slice.Query.RecordsPerPage +
                                ", Count=" + retval.RecordList.Count() +
                                ", QueryString=\"" + slice.Query.ToString() + "\"");
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
                LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                //retval.ErrorList = new string[] { ex.ToString() };
            }
            return retval;
        }

        public void AddPermission(Guid repositoryId, IEnumerable<PermissionItem> list)
        {
            if (list == null) return;
            if (list.Count() == 0) return;
            try
            {
                //TODO: Verify Credentials

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
                        _queryCache.Clear(schema.InternalID, schema.ID);
                    }
                    timer.Stop();
                    //LoggerCQ.LogDebug("AddPermission: Elapsed=" + timer.ElapsedMilliseconds +
                    //    ", LockTime=" + lockTime +
                    //    ", WorkTime=" + (timer.ElapsedMilliseconds - lockTime) +
                    //    ", Count=" + list.Count() +
                    //    ", ID=" + repositoryId + 
                    //    ", Values=" + string.Join(",", list.Select(x => x.UserId + "|" + x.FieldValue)));
                }
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                //retval.ErrorList = new string[] { ex.ToString() };
            }
        }

        public void DeletePermission(Guid repositoryId, IEnumerable<PermissionItem> list)
        {
            try
            {
                //TODO: Verify Credentials

                //Do not call RepositoryExists since this does the same thing
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning("Repository not found: " + repositoryId);
                    //retval.ErrorList = new string[] { "The repository has not been initialized! ID: " + repositoryId };
                    throw new Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException(repositoryId);
                }
                else
                {
                    //LoggerCQ.LogDebug("DeletePermission: Count=" + list.Count() + ", Values=" + string.Join(",", list.Select(x => x.UserId + "|" + x.FieldValue)));
                    using (var q = new AcquireWriterLock(ServerUtilities.RandomizeGuid(repositoryId, RSeedPermissions), "DeletePermission"))
                    {
                        SqlHelper.DeletePermission(schema, list);
                        _queryCache.Clear(schema.InternalID, schema.ID);
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
                //retval.ErrorList = new string[] { ex.ToString() };
            }
        }

        public void ClearPermissions(Guid repositoryId, string fieldValue)
        {
            try
            {
                //TODO: Verify Credentials

                //Do not call RepositoryExists since this does the same thing
                var timer = Stopwatch.StartNew();
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning("Repository not found: " + repositoryId);
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
                            _queryCache.Clear(schema.InternalID, schema.ID);
                        }
                    }
                    timer.Stop();
                    //LoggerCQ.LogDebug("ClearPermissions: Elapsed=" + timer.ElapsedMilliseconds +
                    //    ", LockTime=" + lockTime +
                    //    ", WorkTime=" + (timer.ElapsedMilliseconds - lockTime) +
                    //    ", FieldValue=" + fieldValue);
                }
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                //retval.ErrorList = new string[] { ex.ToString() };
            }
        }

        public void ClearUserPermissions(Guid repositoryId, int userId)
        {
            try
            {
                //TODO: Verify Credentials

                //Do not call RepositoryExists since this does the same thing
                var timer = Stopwatch.StartNew();
                var schema = GetSchema(repositoryId);
                if (schema == null)
                {
                    LoggerCQ.LogWarning("Repository not found: " + repositoryId);
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
                            SqlHelper.ClearUserPermissions(schema, userId);
                            _queryCache.Clear(schema.InternalID, schema.ID);
                        }
                    }
                    timer.Stop();
                    //LoggerCQ.LogDebug("ClearPermissions: Elapsed=" + timer.ElapsedMilliseconds +
                    //    ", LockTime=" + lockTime +
                    //    ", WorkTime=" + (timer.ElapsedMilliseconds - lockTime) +
                    //    ", FieldValue=" + fieldValue);
                }
            }
            catch (Gravitybox.Datastore.Common.Exceptions.RepositoryNotInitializedException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"RepositoryId={repositoryId}");
                //retval.ErrorList = new string[] { ex.ToString() };
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
        private List<DimensionItem> GetAllDimensions(List<long> skipDimensions, int id, DatastoreEntities context, RepositorySchema schema, List<DimensionItem> dimensionList)
        {
            var query = new DataQuery();
            query.IncludeRecords = false;
            query.IncludeEmptyDimensions = true;
            query.SkipDimensions = skipDimensions;
            bool isCore;
            var retval = _queryCache.Get(context, query, id, schema.ID, out isCore);
            if (retval == null)
            {
                retval = SqlHelper.Query(schema, id, query, dimensionList);
                if (query.SkipDimensions.Any())
                    retval.DimensionList.RemoveAll(x => query.SkipDimensions.Contains(x.DIdx));
                _queryCache.Set(context, query, id, schema.ID, retval);
            }
            return retval.DimensionList;
        }

        internal int CacheCount
        {
            get { return _queryCache.Count; }
        }

        RepositorySchema Gravitybox.Datastore.Common.IDataModel.GetSchema(Guid repositoryId)
        {
            return RepositoryManager.GetSchema(repositoryId);
        }

        bool Gravitybox.Datastore.Common.IDataModel.RepositoryExists(Guid repositoryId)
        {
            return RepositoryManager.RepositoryExists(repositoryId);
        }

        private class RepositoryCacheItem
        {
            public string Xml { get; set; }
            public bool HasParent { get; set; }
        }

        private class UpdateDataWhereCacheItem
        {
            public RepositorySchema schema { get; set; }
            public DataQuery query { get; set; }
            public IEnumerable<DataFieldUpdate> list { get; set; }
        }

    }
}