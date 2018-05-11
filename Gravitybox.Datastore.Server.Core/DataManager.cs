using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core
{
    /// <summary>
    /// This will fill in the missing hash values in the data table. If the hash is 0
    /// then this will calculate the data version has and populate the item
    /// </summary>
    internal static class DataManager
    {
        private static long _counter = 0;
        private static System.Timers.Timer _timer = null;
        private static System.Timers.Timer _timerCheck = null;
        private static ConcurrentBag<Guid> _highPriority = new ConcurrentBag<Guid>();
        private static List<Guid> _skipList = new List<Guid>();
        public static readonly Guid FullIndexPatch = new Guid("1da5926a-1900-4FC5-77a4-A897F0035F83");

        #region Constructor

        static DataManager()
        {
#if DEBUG
            const int TimeInterval = 5000;
#else
            const int TimeInterval = 60000;
#endif

            //This will process the Async update where statements
            _timer = new System.Timers.Timer(TimeInterval);
            _timer.Elapsed += _timer_Elapsed;
            _timer.Start();

            _timerCheck = new System.Timers.Timer(TimeInterval);
            _timerCheck.Elapsed += _timerCheck_Elapsed;
            _timerCheck.Start();
        }

#endregion

        public static bool IsActive { get; set; } = true;

        private static bool InFullIndex { get; set; } = false;

        public static void StartFullIndex()
        {
            try
            {
                //If need a full index then add all repositories to the list
                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                {
                    var list = context.Repository
                        .Where(x => x.ParentId == null)
                        .Select(x => x.UniqueKey)
                        .ToList();

                    list.ForEach(x => _highPriority.Add(x));
                }
                InFullIndex = true;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        /// <summary>
        /// Add a repository that needs attention now
        /// </summary>
        /// <returns></returns>
        public static void Add(Guid ID)
        {
            if (!_highPriority.Contains(ID))
                _highPriority.Add(ID);
        }

        public static void AddSkipItem(Guid ID)
        {
            if (!_skipList.Contains(ID))
                _skipList.Add(ID);
        }

        private static void _timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            try
            {
                var value = Interlocked.Read(ref _counter);
                Interlocked.Exchange(ref _counter, 0);
                if (value > 0)
                    LoggerCQ.LogDebug("DataManager: ResetCount=" + value);
            }
            catch { }
        }

        /// <summary>
        /// This timer will trawl through the data and find any item with no hash
        /// It will then then select records to activate the hash generation routine
        /// </summary>
        private static void _timerCheck_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            //If the system is turned off then do nothing
            if (!IsActive) return;

            _timerCheck.Enabled = false;
            try
            {
                const int BlockCount = 50;
                const int WaitTime = 250;

                var core = ((SystemCore)RepositoryManager.SystemCore);
                while (_highPriority.Any())
                {
                    //Try to get an ID from the high priority list
                    //if none found then grab an arbitrary one from the global list
                    Guid ID = Guid.Empty;
                    if (!_highPriority.TryTake(out ID))
                        return;
                    if (ID == Guid.Empty) return;

                    //If there was an error it is in the skip list so never handle this ID again
                    if (!_skipList.Any(x => x == ID))
                    {
                        var sb = new StringBuilder();
                        var schema = RepositoryManager.GetSchema(ID);
                        if (schema != null)
                        {
                            DataQueryResults results = new DataQueryResults();
                            do
                            {
                                if (!IsActive) return; //Stop when housekeeping comes on
                                var query = new DataQuery { IncludeRecords = true, IncludeDimensions = false, ExcludeCount = true };
                                query.FieldFilters.Add(new FieldFilter { Name = SqlHelper.HashField, Comparer = ComparisonConstants.Equals, Value = 0 });
                                query.RecordsPerPage = BlockCount;
                                try
                                {
                                    results = core.Manager.Query(ID, query, true);
                                    Interlocked.Add(ref _counter, results.RecordList.Count);
                                }
                                catch
                                {
                                    results = new DataQueryResults();
                                }

                                //Do not overload the system with background queries
                                if (results.ComputeTime > 2000)
                                    System.Threading.Thread.Sleep(WaitTime * 2);
                                else if (core.GetCpu() > 60)
                                    System.Threading.Thread.Sleep(WaitTime * 8);
                                else
                                    System.Threading.Thread.Sleep(WaitTime);

                            } while (results.RecordList?.Count == BlockCount && results.ComputeTime < 5000);
                        }
                    }
                }

                if (InFullIndex)
                    SystemCore.PatchApply(FullIndexPatch, "DataManager:FullIndex");
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
            finally
            {
                _timerCheck.Enabled = true;
            }
        }

        public static void Sync(List<DataItem> list, RepositorySchema schema)
        {
            try
            {
                Task.Factory.StartNew(() =>
                {
                    SyncInternal(list, schema);
                });
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        private static void SyncInternal(List<DataItem> list, RepositorySchema schema)
        {
            try
            {
                if (list == null) return;
                if (schema == null) return;

                var l = list.Where(x => x.__Hash == 0 && x.__RecordIndex > 0).ToList();
                if (!l.Any()) return;

                var dataTable = SqlHelper.GetTableName(schema.ID);
                foreach (var item in l)
                {
                    var sb = new StringBuilder();
                    var parameters = new List<SqlParameter>();

                    parameters.Add(new SqlParameter
                    {
                        DbType = DbType.Int64,
                        IsNullable = false,
                        ParameterName = $"@{SqlHelper.HashField}",
                        Value = item.Hash(),
                    });

                    parameters.Add(new SqlParameter
                    {
                        DbType = DbType.Int64,
                        IsNullable = false,
                        ParameterName = $"@{SqlHelper.RecordIdxField}",
                        Value = item.__RecordIndex,
                    });

                    sb.AppendLine($"UPDATE [{dataTable}] SET [{SqlHelper.HashField}] = @{SqlHelper.HashField} WHERE [{SqlHelper.RecordIdxField}] = @{SqlHelper.RecordIdxField} AND [{SqlHelper.HashField}] = 0");
                    SqlHelper.ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), parameters, false, false);
                    Interlocked.Increment(ref _counter);
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }
    }
}