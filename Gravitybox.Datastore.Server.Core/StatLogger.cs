#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;
using Gravitybox.Datastore.EFDAL.Entity;

namespace Gravitybox.Datastore.Server.Core
{
    internal static class StatLogger
    {
        private const int LOG_TIMER_INTERVAL = 30 * 1000;
        private static bool _ready = false;
        private static DateTime _pivot = new DateTime(2000, 1, 1);
        private static Dictionary<Guid, List<RepositorySummmaryStats>> _statCache = new Dictionary<Guid, List<RepositorySummmaryStats>>();
        private static System.Timers.Timer _timer = null;

        public const string PERFMON_CATEGORY = "HP Datastore";
        public const string COUNTER_MEMUSAGE = "Memory usage";
        public const string COUNTER_LOADDELTA = "Repositories loads/interval";
        public const string COUNTER_UNLOADDELTA = "Repositories unloads/interval";
        public const string COUNTER_REPOTOTAL = "Repositories total";
        public const string COUNTER_REPOCREATE = "Repositories creates/interval";
        public const string COUNTER_REPODELETE = "Repositories deletes/interval";

        #region Init

        public static void Initialize()
        {
            try
            {
                //Log repository stats every N seconds
                _timer = new System.Timers.Timer(LOG_TIMER_INTERVAL);
                _timer.AutoReset = false;
                _timer.Elapsed += TimerTick;
                _timer.Start();

                _ready = true;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void Shutdown()
        {
            try
            {
                _timer.Stop();
                TimerTick(null, null);
                _timer.Stop();
            }
            catch (Exception ex)
            {
                //throw;
            }
        }

        #endregion

        #region Server Stats

        public static void Log(RealtimeStats item)
        {
            if (!_ready) return;
            try
            {
                //TODO: Log Stat
                //using (var context = new DataCoreEntities())
                //{
                //    var newItem = new ServerStat()
                //                  {
                //                      MemoryUsageTotal = item.MemoryUsageTotal,
                //                      MemoryUsageAvailable = item.MemoryUsageAvailable,
                //                      MemoryUsageProcess = item.MemoryUsageProcess,
                //                      RepositoryInMem = item.RepositoryInMem,
                //                      RepositoryLoadDelta = item.RepositoryLoadDelta,
                //                      RepositoryUnloadDelta = item.RepositoryUnloadDelta,
                //                      RepositoryTotal = item.RepositoryTotal,
                //                      RepositoryCreateDelta = item.RepositoryCreateDelta,
                //                      RepositoryDeleteDelta = item.RepositoryDeleteDelta,
                //                      ProcessorUsage = item.ProcessorUsage,
                //                  };
                //    context.AddItem(newItem);
                //    context.SaveChanges();
                //}
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static List<RealtimeStats> QueryServerStats(DateTime start, DateTime end)
        {
            if (!_ready) return null;

            try
            {
                var retval = new List<RealtimeStats>();

                //TODO: Query stats
                //using (var context = new DataCoreEntities())
                //{
                //    var list = context.ServerStat
                //        .Where(x => start <= x.AddedDate && x.AddedDate < end).OrderBy(x => x.AddedDate)
                //        .ToList();
                //    foreach (var item in list)
                //    {
                //        retval.Add(new RealtimeStats
                //                   {
                //                       Timestamp = item.AddedDate,
                //                       MemoryUsageAvailable = item.MemoryUsageAvailable,
                //                       MemoryUsageProcess = item.MemoryUsageProcess,
                //                       MemoryUsageTotal = item.MemoryUsageTotal,
                //                       ProcessorUsage = item.ProcessorUsage,
                //                       RepositoryCreateDelta = (int)item.RepositoryCreateDelta,
                //                       RepositoryDeleteDelta = (int)item.RepositoryDeleteDelta,
                //                       RepositoryInMem = item.RepositoryInMem,
                //                       RepositoryLoadDelta = item.RepositoryLoadDelta,
                //                       RepositoryTotal = item.RepositoryTotal,
                //                       RepositoryUnloadDelta = item.RepositoryUnloadDelta,
                //                   });
                //    }
                //}

                return retval;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return null;
            }
        }

        #endregion

        #region Repository Stats

        private static void TimerTick(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (!_ready) return;

            _timer.Stop();
            try
            {
                //Lock the stats list and build queries
                Dictionary<Guid, List<RepositorySummmaryStats>> copyCache = null;
                lock (_statCache)
                {
                    copyCache = _statCache.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                    _statCache.Clear();
                }

                var repositoryCache = new Dictionary<Guid, Repository>();
                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                {
                    var typeValues = Enum.GetValues(typeof(RepositoryActionTypeConstants)).Cast<int>().ToList();
                    foreach (var typeId in typeValues)
                    {
                        foreach (var key in copyCache.Keys)
                        {
                            var q = (RepositoryActionConstants)typeId;
                            var queryList = copyCache[key].Where(x => x.ActionType == q).ToList();
                            var elapsed = queryList.Sum(x => x.Elapsed); //Total elapsed time
                            var lockTime = queryList.Sum(x => x.LockTime); //Total lock time
                            var waitingLocks = queryList.Sum(x => x.WaitingLocksOnEntry); //Total write locks on entry
                            var readLockCount = queryList.Sum(x => x.ReadLockCount); //Total read locks on entry
                            var count = queryList.Count; //Number of queries
                            var itemCount = 0;
                            if (queryList.Count > 0)
                                itemCount = queryList.Sum(x => x.ItemCount);

                            //Ensure repository still exists (may have been removed in interim)
                            Repository repository = null;
                            if (repositoryCache.ContainsKey(key)) repository = repositoryCache[key];
                            else
                            {
                                repository = context.Repository.FirstOrDefault(x => x.UniqueKey == key);
                                repositoryCache[key] = repository;
                            }

                            if (repository != null && (count > 0 || elapsed > 0 || itemCount > 0))
                            {
                                var newItem = new RepositoryStat()
                                              {
                                                  Count = count,
                                                  Elapsed = elapsed,
                                                  LockTime = lockTime,
                                                  ItemCount = itemCount,
                                                  RepositoryActionTypeId = typeId,
                                                  RepositoryId = repository.RepositoryId,
                                                  WaitingLocks = waitingLocks,
                                                  ReadLockCount = readLockCount,
                                };
                                context.AddItem(newItem);
                            }
                        }
                    }
                    context.SaveChanges();
                }

            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
            finally
            {
                _timer.Start();
            }
        }

        public static void Log(RepositorySummmaryStats item)
        {
            try
            {
                lock (_statCache)
                {
                    if (!_statCache.ContainsKey(item.RepositoryId))
                        _statCache.Add(item.RepositoryId, new List<RepositorySummmaryStats>());
                    _statCache[item.RepositoryId].Add(item);
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static RepositorySummmaryStats QueryRepositoryStats(Guid repositoryId, DateTime start, DateTime end)
        {
            if (!_ready) return null;

            try
            {
                var retval = new RepositorySummmaryStats() { ActionType = RepositoryActionConstants.Query };

                //TODO: Query stats
                //using (var context = new DataCoreEntities())
                //{
                //    var repository = context.RepositoryDefinition.FirstOrDefault(x => x.UniqueKey == repositoryId);
                //    if (repository == null)
                //        throw new Exception("Unknown Repository");

                //    var actionId = (int)RepositoryActionConstants.Query;
                //    var lambda = context.RepositoryStat.Where(x => start <= x.CreatedDate &&
                //        x.CreatedDate < end &&
                //        (repositoryId == Guid.Empty || repository.RepositoryId == x.RepositoryId) &&
                //        x.RepositoryActionTypeId == actionId)
                //        .OrderBy(x => x.CreatedDate);

                //    long count = 0;
                //    long totalElapsed = 0;
                //    if (lambda.Any())
                //    {
                //        count = lambda.Sum(x => x.Count);
                //        totalElapsed = lambda.Sum(x => x.Elapsed);
                //    }

                //    var elapsedPer = 0.0;
                //    if (count > 0) elapsedPer = ((totalElapsed * 1.0) / count);

                //    retval.ItemCount = (int)count;
                //    retval.Elapsed = (int)elapsedPer;
                //    retval.RepositoryId = repositoryId;
                //}

                return retval;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return null;
            }
        }

        #endregion
    }
}
