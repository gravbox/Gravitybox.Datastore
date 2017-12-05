#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Gravitybox.Datastore.Common;
using System.ServiceModel;
using System.Runtime.Serialization;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Gravitybox.Datastore.EFDAL;
using Gravitybox.Datastore.EFDAL.Entity;
using System.Xml;
using System.IO.Compression;

namespace Gravitybox.Datastore.Server.Core
{
    [Serializable()]
    [KnownType(typeof(RepositorySchema))]
    [KnownType(typeof(FieldDefinition))]
    [KnownType(typeof(DimensionDefinition))]
    [KnownType(typeof(IFieldDefinition))]
    [KnownType(typeof(Gravitybox.Datastore.Common.IRemotingObject))]
    [KnownType(typeof(Gravitybox.Datastore.Common.BaseRemotingObject))]
    [ServiceBehavior(ConcurrencyMode = ConcurrencyMode.Multiple, InstanceContextMode = InstanceContextMode.Single)]
    public class SystemCore : MarshalByRefObject, ISystemCore, IDisposable
    {
        #region Win32 Callout

        [DllImport("psapi.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool GetPerformanceInfo([Out] out PerformanceInformation PerformanceInformation, [In] int Size);

        [StructLayout(LayoutKind.Sequential)]
        public struct PerformanceInformation
        {
            public int Size;
            public IntPtr CommitTotal;
            public IntPtr CommitLimit;
            public IntPtr CommitPeak;
            public IntPtr PhysicalTotal;
            public IntPtr PhysicalAvailable;
            public IntPtr SystemCache;
            public IntPtr KernelTotal;
            public IntPtr KernelPaged;
            public IntPtr KernelNonPaged;
            public IntPtr PageSize;
            public int HandlesCount;
            public int ProcessCount;
            public int ThreadCount;
        }

        #endregion

        #region Class Members

        private System.Timers.Timer _timerHouseKeeping = null;
        private System.Timers.Timer _timerStats = null;
        private DateTime _lastServerStatWrite;
        private System.Diagnostics.PerformanceCounter _cpuCounter;
        private Dictionary<Guid, int> _loadDelta = new Dictionary<Guid, int>();
        private Dictionary<Guid, int> _unloadDelta = new Dictionary<Guid, int>();
        private int _createdDelta = 0;
        private int _deletedDelta = 0;
        private RepositoryManager _manager = null;
        private bool _allowCounters = false;
        private DateTime _lastLogSend;
        private System.Timers.Timer _timer = null;
        private bool _isSystemReady = false;
        private Stopwatch _startupTimer = Stopwatch.StartNew();
        public static long LastMemoryUsage = 0;

        #endregion

        #region Constructors

        public SystemCore(string connectionString)
            : base()
        {
            try
            {
                var builder = new System.Data.SqlClient.SqlConnectionStringBuilder(connectionString);
                LoggerCQ.LogInfo("Connection: Server=" + builder.DataSource + ", Database=" + builder.InitialCatalog);

                ConfigHelper.ConnectionString = connectionString;
                ConfigHelper.SupportsCompression = SqlHelper.IsEnterpiseVersion(connectionString);
                ConfigHelper.SqlVersion = SqlHelper.GetSqlVersion(connectionString);

                if (!SqlHelper.CanConnect(ConfigHelper.ConnectionString))
                    throw new Exception("Connection string is not valid");
                if (!SqlHelper.HasFTS(ConfigHelper.ConnectionString))
                    throw new Exception("The database must have full text search enabled");

                _manager = new RepositoryManager(this);
                DataManager.IsActive = true;
                QueryBuilders.RepositoryHealthMonitor.IsActive = true;
                //this.StatLocker = new DatastoreLock(System.Threading.LockRecursionPolicy.SupportsRecursion);
                //this.StatLocker = new DatastoreLock();
                _lastLogSend = DateTime.Now.Date;

                StatLogger.Initialize();
                LockLogger.Initialize();

                /*
                // GetDiskInfo assumes Datastore service is running on the same machine as the database server.
                // When connecting to a remote database server, it produces incorrect results and may cause the
                // Datastore service to crash if the database server uses a physical drive that does not exist
                // on the Datastore machine.
                var diskInfoList = SqlHelper.GetDiskInfo(connectionString);
                foreach (var item in diskInfoList)
                {
                    LoggerCQ.LogInfo("Disk Info " + item.Item1 + " Available=" + item.Item2.ToString("N0"));
                }
*/

                this.SetupCounters();

                LoggerCQ.LogInfo("Repositories: Count=" + GetRepositoryCount(new PagingInfo()));
                LoggerCQ.LogInfo("Core initialize: Server=" + Environment.MachineName + ", Mode=x" + (Environment.Is64BitProcess ? "64" : "32") + ", Port=" + ConfigHelper.Port);
                LoggerCQ.LogInfo("SqlVersion: " + ConfigHelper.SqlVersion);
                LoggerCQ.LogInfo("SupportsRowsFetch: " + ConfigHelper.SupportsRowsFetch);
                LoggerCQ.LogInfo("SupportsCompression: " + ConfigHelper.SupportsCompression);
                LoggerCQ.LogInfo("AllowCaching: " + ConfigHelper.AllowCaching);
                LoggerCQ.LogInfo("AllowLocking: " + ConfigHelper.AllowLocking);
                LoggerCQ.LogInfo("DefragIndexes: " + ConfigHelper.DefragIndexes);
                LoggerCQ.LogInfo("AsyncCachePath: " + ConfigHelper.AsyncCachePath);
                LoggerCQ.LogInfo("AllowLockStats: " + ConfigHelper.AllowLockStats);
                LoggerCQ.LogInfo("Timezone: " + TimeZone.CurrentTimeZone.DaylightName);
#if DEBUG
                LoggerCQ.LogInfo("Build: Debug");
#else
                    LoggerCQ.LogInfo("Build: Release");
#endif

                try
                {
                    //Default to temp folder
                    if (string.IsNullOrEmpty(ConfigHelper.AsyncCachePath))
                    {
                        ConfigHelper.AsyncCachePath = Path.GetTempPath();
                    }

                    if (!string.IsNullOrEmpty(ConfigHelper.AsyncCachePath))
                    {
                        if (!Directory.Exists(ConfigHelper.AsyncCachePath))
                            Directory.CreateDirectory(ConfigHelper.AsyncCachePath);
                    }
                }
                catch (Exception ex)
                {
                    LoggerCQ.LogWarning("Cannot create path '" + ConfigHelper.AsyncCachePath + "'");
                    throw;
                }

                #region Force save of configuration
                ConfigHelper.AllowCaching = ConfigHelper.AllowCaching;
                ConfigHelper.AllowLocking = ConfigHelper.AllowLocking;
                ConfigHelper.QueryCacheCount = ConfigHelper.QueryCacheCount;
                ConfigHelper.DefragIndexes = ConfigHelper.DefragIndexes;
                ConfigHelper.FromEmail = ConfigHelper.FromEmail;
                ConfigHelper.DebugEmail = ConfigHelper.DebugEmail;
                ConfigHelper.MailServerUsername = ConfigHelper.MailServerUsername;
                ConfigHelper.MailServerPort = ConfigHelper.MailServerPort;
                ConfigHelper.MailServer = ConfigHelper.MailServer;
                ConfigHelper.Port = ConfigHelper.Port;
                #endregion

                using (var q = new AcquireWriterLock(Guid.Empty, "DelayStartup"))
                {
                    //Verify that all permissions are in place
                    if (!this.IsSetupValid())
                    {
                        throw new Exception("This application does not have the proper permissions!");
                    }

                    var a = System.Reflection.Assembly.GetExecutingAssembly();
                    var version = a.GetName().Version;
                    var d = System.IO.File.GetLastWriteTime(a.Location);
                    LoggerCQ.LogInfo("Manager Started: Version=" + version.ToString() + ", Compiled=" + d.ToString("yyyy-MM-dd HH:mm:ss"));
                }

                _cpuCounter = new System.Diagnostics.PerformanceCounter();
                _cpuCounter.CategoryName = "Processor";
                _cpuCounter.CounterName = "% Processor Time";
                _cpuCounter.InstanceName = "_Total";

                //Every 30 seconds tell server that machine is still alive
                _timer = new System.Timers.Timer(30000);
                _timer.Elapsed += _timer_Elapsed;
                _timer.Start();

                _lastServerStatWrite = DateTime.MinValue;
                _timerStats = new System.Timers.Timer(5000);
                _timerStats.Elapsed += _timerStats_Elapsed;
                _timerStats.Start();

                //Every 5 minutes perform housekeeping
#if DEBUG
                _timerHouseKeeping = new System.Timers.Timer(30000);
#else
                _timerHouseKeeping = new System.Timers.Timer(300000);
#endif
                _timerHouseKeeping.Elapsed += _timerHouseKeeping_Elapsed;
                _timerHouseKeeping.Start();

                var patchThread = new System.Threading.Thread(InitializeService);
                patchThread.Start();

            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        private void InitializeService()
        {
            try
            {
                #region Apply Fixes
                LoggerCQ.LogInfo("Apply fixes begin");

                var currentGuid = new Guid("0152DFCB-C94A-4FC5-8DD2-A897F0035B4D");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: ListTableRecordIndex");
                    PatchesDomain.ApplyFix_ListTableRecordIndex(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "ListTableRecordIndex");
                }

                currentGuid = new Guid("0223DFCB-C94A-4FC5-8DD2-A897F0035B4D");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: MakePKNonClustered");
                    PatchesDomain.ApplyFix_MakePKNonClustered(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "MakePKNonClustered");
                }

                currentGuid = new Guid("5557DFCB-C94A-4FC5-8DD2-A897F0035B4D");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: EnsureIndexes");
                    PatchesDomain.ApplyFix_EnsureIndexes(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "EnsureIndexes");
                }

                currentGuid = new Guid("7731DFCB-C94A-4FC5-8DD2-A897F0035BEE");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: CompressTables-A");
                    PatchesDomain.ApplyFix_CompressTablesA(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "CompressTables-A");
                }

                currentGuid = new Guid("1981DFCB-C94A-4FC5-8DD2-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: Remove Lazy Delete");
                    PatchesDomain.ApplyFix_RemoveLazyDelete(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Remove Lazy Delete");
                }

                currentGuid = new Guid("8824DFCB-C94A-4FC5-8DD2-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: Log Optimize");
                    PatchesDomain.ApplyFix_LogOptimize(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Log Optimize");
                }

                currentGuid = new Guid("992715CB-C94A-4FC5-8DD2-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: DimensionTables");
                    PatchesDomain.ApplyFix_DimensionTables(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Dimension Tables");
                }

                currentGuid = new Guid("180315CB-C94A-4FC5-8DD2-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: Add Timestamp");
                    PatchesDomain.ApplyFix_AddTimestamp(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Add Timestamp");
                }

                currentGuid = new Guid("991815CB-C94A-4FC5-8DD2-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: Add Unique PK Index");
                    PatchesDomain.ApplyFix_AddUniquePKIndex(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Add Unique PK Index");
                }

                currentGuid = new Guid("71fa15CB-C94A-4FC5-8DD2-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: Make DVIdx Long");
                    PatchesDomain.ApplyFix_DVIdxMakeLong(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Make DVIdx Long");
                }

                currentGuid = new Guid("18aae425-C94A-4FC5-8DD2-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: Fix X-Table index");
                    PatchesDomain.ApplyFix_XTableMultiKey(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Fix X-Table index");
                }

                currentGuid = new Guid("89054325-C94A-4FC5-8DD2-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: Compress Timestamps");
                    PatchesDomain.ApplyFix_CompressTimestamp(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Compress Timestamps");
                }

                currentGuid = new Guid("81905425-C94A-4FC5-8DD2-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: Fix XYIndexes");
                    PatchesDomain.ApplyFix_XYIndexes(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Fix XYIndexes");
                }

                currentGuid = new Guid("01764253-C94A-4FC5-8DD2-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: Fix AddYIndex2");
                    PatchesDomain.ApplyFix_AddYIndex2(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Fix AddYIndex2");
                }

                currentGuid = new Guid("a7d4199e-C94A-4FC5-8DD2-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: Fix ChangeYPKType");
                    PatchesDomain.ApplyFix_ChangeYPKType(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Fix ChangeYPKType");
                }

                currentGuid = new Guid("11825001-17a4-4FC5-77a4-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: Fix Reorg FTS");
                    var b = PatchesDomain.ApplyFix_ReorgFTS(ConfigHelper.ConnectionString, _manager);
                    if (b)
                    {
                        PatchApply(currentGuid, "Fix ReorgFTS");
                    }
                }

                currentGuid = new Guid("77d35001-1900-4FC5-77a4-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: AddZHash");
                    PatchesDomain.ApplyFix_AddZHash(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Fix AddZHash");
                }

                currentGuid = new Guid("a8891f04-1900-4FC5-77a4-A897F0035F83");
                if (!IsPatchApplied(currentGuid))
                {
                    LoggerCQ.LogInfo("Applying fix: IndexOptimization");
                    PatchesDomain.ApplyFix_IndexOptimization(ConfigHelper.ConnectionString);
                    PatchApply(currentGuid, "Fix IndexOptimization");
                }

                if (!IsPatchApplied(DataManager.FullIndexPatch))
                {
                    DataManager.StartFullIndex();
                }

                //Ensure repositories are in tact
                PatchesDomain.ApplyFix_RepositoryIntegrity(ConfigHelper.ConnectionString);

                LoggerCQ.LogInfo("Apply fixes end");
                #endregion

                SqlHelper.VerifyTablesExists(ConfigHelper.ConnectionString);

                //The system is now ready to be used
                _isSystemReady = true;

                _startupTimer.Stop();
                LoggerCQ.LogInfo("Core initialize complete: Elapsed=" + _startupTimer.ElapsedMilliseconds);
                _startupTimer = null;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        internal static bool IsPatchApplied(Guid patchId)
        {
            using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
            {
                return context.AppliedPatch.Any(x => x.ID == patchId);
            }
        }

        internal static void PatchApply(Guid patchId, string name)
        {
            using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
            {
                if (!context.AppliedPatch.Any(x => x.ID == patchId))
                {
                    context.AddItem(new AppliedPatch
                    {
                        ID = patchId,
                        Description = name,
                    });
                    context.SaveChanges();
                }
            }
        }

        private DateTime _lastHouseKeeping = DateTime.MinValue;
        private void _timerHouseKeeping_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            _timerHouseKeeping.Stop();
            DataManager.IsActive = false;
            try
            {
                //If been more than 12 hours and it is between 3-4am and not run in the last 6 hours then run housekeeping
                if (((DateTime.Now.Subtract(ConfigHelper.LastDefrag).TotalHours > 12) && (DateTime.Now.Hour == 3) && DateTime.Now.Subtract(_lastHouseKeeping).TotalHours > 6))
                {
                    _lastHouseKeeping = DateTime.Now;
                    LoggerCQ.LogInfo("Housekeeping Start");
                    SqlHelper.CleanLogs(ConfigHelper.ConnectionString);
                    SqlHelper.DefragIndexes(ConfigHelper.ConnectionString);
                    SqlHelper.DefragFTS(ConfigHelper.ConnectionString);
                    SqlHelper.LogRepositoryStats(ConfigHelper.ConnectionString);
                    SqlHelper.ClearCache(ConfigHelper.ConnectionString);
                    LoggerCQ.LogInfo("Housekeeping End");
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogWarning("Housekeeping timer failed");
            }
            finally
            {
                DataManager.IsActive = true;
                _timerHouseKeeping.Start();
            }
        }

        private void _timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            _timer.Stop();
            try
            {
                //Gravitybox.Datastore.Server.Core.DatastoreLock.ServerAlive();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogWarning("Alive timer failed");
            }
            finally
            {
                _timer.Start();
            }
        }

        private void SetupCounters()
        {
            try
            {
                var CounterDatas = new System.Diagnostics.CounterCreationDataCollection();

                //if (System.Diagnostics.PerformanceCounterCategory.Exists(StatLogger.PERFMON_CATEGORY))
                //{
                //    System.Diagnostics.PerformanceCounterCategory.Delete(StatLogger.PERFMON_CATEGORY);
                //}

                if (!System.Diagnostics.PerformanceCounterCategory.Exists(StatLogger.PERFMON_CATEGORY))
                {
                    //Memory Usage Process
                    var cMemoryUsageProcess = new System.Diagnostics.CounterCreationData();
                    cMemoryUsageProcess.CounterName = StatLogger.COUNTER_MEMUSAGE;
                    cMemoryUsageProcess.CounterHelp = "Total memory used by the service";
                    cMemoryUsageProcess.CounterType = PerformanceCounterType.NumberOfItems64;
                    CounterDatas.Add(cMemoryUsageProcess);

                    //Repository Load Delta
                    var cRepositoryLoadDelta = new System.Diagnostics.CounterCreationData();
                    cRepositoryLoadDelta.CounterName = StatLogger.COUNTER_LOADDELTA;
                    cRepositoryLoadDelta.CounterHelp = "Number of repository loads/interval";
                    cRepositoryLoadDelta.CounterType = PerformanceCounterType.RateOfCountsPerSecond32;
                    CounterDatas.Add(cRepositoryLoadDelta);

                    //Repository Unload Delta
                    var cRepositoryUnloadDelta = new System.Diagnostics.CounterCreationData();
                    cRepositoryUnloadDelta.CounterName = StatLogger.COUNTER_UNLOADDELTA;
                    cRepositoryUnloadDelta.CounterHelp = "Number of repository unloads/interval";
                    cRepositoryUnloadDelta.CounterType = PerformanceCounterType.RateOfCountsPerSecond32;
                    CounterDatas.Add(cRepositoryUnloadDelta);

                    //Repository Total
                    var cRepositoryTotal = new System.Diagnostics.CounterCreationData();
                    cRepositoryTotal.CounterName = StatLogger.COUNTER_REPOTOTAL;
                    cRepositoryTotal.CounterHelp = "Total number of system repositories";
                    cRepositoryTotal.CounterType = PerformanceCounterType.NumberOfItems32;
                    CounterDatas.Add(cRepositoryTotal);

                    //Repository Create Delta
                    var cRepositoryCreateDelta = new System.Diagnostics.CounterCreationData();
                    cRepositoryCreateDelta.CounterName = StatLogger.COUNTER_REPOCREATE;
                    cRepositoryCreateDelta.CounterHelp = "Number of repository creates/interval";
                    cRepositoryCreateDelta.CounterType = PerformanceCounterType.RateOfCountsPerSecond32;
                    CounterDatas.Add(cRepositoryCreateDelta);

                    //Repository Delete Delta
                    var cRepositoryDeleteDelta = new System.Diagnostics.CounterCreationData();
                    cRepositoryDeleteDelta.CounterName = StatLogger.COUNTER_REPODELETE;
                    cRepositoryDeleteDelta.CounterHelp = "Number of repository deletes/interval";
                    cRepositoryDeleteDelta.CounterType = PerformanceCounterType.RateOfCountsPerSecond32;
                    CounterDatas.Add(cRepositoryDeleteDelta);

                    //Add all counters
                    System.Diagnostics.PerformanceCounterCategory.Create(StatLogger.PERFMON_CATEGORY, "Metrics for the HP Datastore faceted navigation engine",
                        PerformanceCounterCategoryType.SingleInstance, CounterDatas);
                }
                _allowCounters = true;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogInfo("The service does not have permission to create performance counters!");
                _allowCounters = false;
            }

        }

        public RepositoryManager Manager
        {
            get { return _manager; }
        }

        #endregion

        #region Event Handlers

        private long GetProcessMemory()
        {
            try
            {
                var p = Process.GetProcessesByName(System.Reflection.Assembly.GetEntryAssembly().GetName().Name).FirstOrDefault();
                if (p != null) return p.PrivateMemorySize64;
                return 0;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return 0;
            }
        }

        private DateTime _lastStatDbCall = DateTime.MinValue;
        private int _lastLoadDeltaPerfmonSummary = 0;
        private int _lastUnloadDeltaPerfmonSummary = 0;
        private int _lastCreateDeltaPerfmonSummary = 0;
        private int _lastDeleteDeltaPerfmonSummary = 0;

        public static int LastProcessor { get; private set; }
        public const int ProcessorThreshold = 80;

        private void _timerStats_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            //This will prevent time drift
            if (DateTime.Now.Subtract(_lastServerStatWrite).TotalMinutes < 1) return;
            _timerStats.Stop();
            try
            {
                _lastServerStatWrite = DateTime.Now;
                _lastServerStatWrite = _lastServerStatWrite.AddSeconds(-_lastServerStatWrite.Second).AddMilliseconds(-_lastServerStatWrite.Millisecond);

                #region CPU

                try
                {
                    LastProcessor = GetCpu();
                }
                catch { }

                #endregion

                #region Perf Counter

                var info = new PerformanceInformation();
                GetPerformanceInfo(out info, Marshal.SizeOf(info));

                #endregion

                #region Write stat
                var timer = Stopwatch.StartNew();
                timer.Start();
                //using (var q = new AcquireWriterLock(_manager.SyncObject, "_timerStats_Elapsed"))
                {
                    var newItem = new RealtimeStats
                    {
                        Timestamp = _lastServerStatWrite,
                        MemoryUsageAvailable = info.PhysicalAvailable.ToInt64() * info.PageSize.ToInt64(),
                        MemoryUsageProcess = GetProcessMemory(),
                        MemoryUsageTotal = info.PhysicalTotal.ToInt64() * info.PageSize.ToInt64(),
                        ProcessorUsage = LastProcessor,
                        RepositoryLoadDelta = _loadDelta.Keys.Count,
                        RepositoryUnloadDelta = _unloadDelta.Keys.Count,
                        RepositoryTotal = GetRepositoryCount(new PagingInfo()),
                        RepositoryCreateDelta = _createdDelta,
                        RepositoryDeleteDelta = _deletedDelta,
                    };

                    //Log to database once per minute
                    if (DateTime.Now.Subtract(_lastStatDbCall).TotalSeconds >= 60)
                    {
                        _lastStatDbCall = DateTime.Now;
                        StatLogger.Log(newItem);

                        _loadDelta = new Dictionary<Guid, int>();
                        _unloadDelta = new Dictionary<Guid, int>();
                        _createdDelta = 0;
                        _deletedDelta = 0;
                    }

                    //Log Performance counters
                    try
                    {
                        if (_allowCounters)
                        {
                            (new System.Diagnostics.PerformanceCounter(StatLogger.PERFMON_CATEGORY, StatLogger.COUNTER_MEMUSAGE, string.Empty, false)).RawValue = newItem.MemoryUsageProcess;
                            (new System.Diagnostics.PerformanceCounter(StatLogger.PERFMON_CATEGORY, StatLogger.COUNTER_LOADDELTA, string.Empty, false)).RawValue = newItem.RepositoryLoadDelta - _lastLoadDeltaPerfmonSummary;
                            (new System.Diagnostics.PerformanceCounter(StatLogger.PERFMON_CATEGORY, StatLogger.COUNTER_UNLOADDELTA, string.Empty, false)).RawValue = newItem.RepositoryUnloadDelta - _lastUnloadDeltaPerfmonSummary;
                            (new System.Diagnostics.PerformanceCounter(StatLogger.PERFMON_CATEGORY, StatLogger.COUNTER_REPOTOTAL, string.Empty, false)).RawValue = newItem.RepositoryTotal;
                            (new System.Diagnostics.PerformanceCounter(StatLogger.PERFMON_CATEGORY, StatLogger.COUNTER_REPOCREATE, string.Empty, false)).RawValue = newItem.RepositoryCreateDelta - _lastCreateDeltaPerfmonSummary;
                            (new System.Diagnostics.PerformanceCounter(StatLogger.PERFMON_CATEGORY, StatLogger.COUNTER_REPODELETE, string.Empty, false)).RawValue = newItem.RepositoryDeleteDelta - _lastDeleteDeltaPerfmonSummary;
                        }
                    }
                    catch (Exception ex)
                    {
                        //Do Nothing
                    }

                    //Reset delta lists for perf counters
                    _lastLoadDeltaPerfmonSummary = _loadDelta.Count;
                    _lastUnloadDeltaPerfmonSummary = _unloadDelta.Count;
                    _lastCreateDeltaPerfmonSummary = _createdDelta;
                    _lastDeleteDeltaPerfmonSummary = _deletedDelta;

                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        //Create the server if it does not exist
                        var server = context.Server.FirstOrDefault(x => x.Name == Environment.MachineName);
                        if (server == null)
                            context.AddItem(new Gravitybox.Datastore.EFDAL.Entity.Server()
                            {
                                Name = Environment.MachineName,
                            });

                        var dbItem = new ServerStat();
                        dbItem.AddedDate = newItem.Timestamp;
                        dbItem.MemoryUsageAvailable = newItem.MemoryUsageAvailable;
                        dbItem.MemoryUsageProcess = newItem.MemoryUsageProcess;
                        dbItem.MemoryUsageTotal = newItem.MemoryUsageTotal;
                        dbItem.ProcessorUsage = newItem.ProcessorUsage;
                        dbItem.RepositoryCreateDelta = newItem.RepositoryCreateDelta;
                        dbItem.RepositoryDeleteDelta = newItem.RepositoryDeleteDelta;
                        dbItem.RepositoryLoadDelta = newItem.RepositoryLoadDelta;
                        dbItem.RepositoryTotal = newItem.RepositoryTotal;
                        dbItem.RepositoryUnloadDelta = newItem.RepositoryUnloadDelta;
                        dbItem.CachedItems = _manager.CacheCount;
                        dbItem.Server = server;
                        context.AddItem(dbItem);
                        context.SaveChanges();
                    }
                    SystemCore.LastMemoryUsage = newItem.MemoryUsageProcess;

                }
                timer.Stop();
                //LoggerCQ.LogDebug("_timerStats_Elapsed: Elapsed=" + timer.ElapsedMilliseconds);
                #endregion

                //If we have not sent the log today then send it
                if (_lastLogSend != DateTime.Now.Date && !string.IsNullOrEmpty(ConfigHelper.DebugEmail))
                {
                    _lastLogSend = DateTime.Now.Date;
                    SendLogs();
                }

            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
            finally
            {
                _timerStats.Start();
            }
        }

        public int GetCpu()
        {
            try
            {
                return (int)_cpuCounter.NextValue();
            }
            catch (Exception ex)
            {
                return 0;
            }
        }

        private void SendLogs()
        {
            if (string.IsNullOrEmpty(ConfigHelper.DebugEmail)) return;
            try
            {
                var logDate = DateTime.Now.Date.AddDays(-1);
                var logFile = Path.Combine((new FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location)).DirectoryName, "logs", logDate.ToString("yyyy-MM-dd")) + ".txt";
                if (!File.Exists(logFile)) return;
                var serverStatFile = GetServerStatDumpFile(logDate);
                var zipfile = Path.Combine(Path.GetTempPath(), logDate.ToString("yyyy-MM-dd") + ".logs.zip");
                if (File.Exists(zipfile))
                {
                    File.Delete(zipfile);
                    System.Threading.Thread.Sleep(500);
                }
                AddToZip(zipfile, logFile);
                AddToZip(zipfile, serverStatFile);

                EmailDomain.SendMail(new EmailSettings
                {
                    Body = string.Empty,
                    From = ConfigHelper.FromEmail,
                    Subject = "Logs for " + logDate.ToString("yyyy-MM-dd") + " [" + Environment.MachineName + "]",
                    To = ConfigHelper.DebugEmail,
                    Attachments = new List<string>() { zipfile },
                });

                File.Delete(serverStatFile);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        internal static void AddToZip(string zipfile, string sourceFile)
        {
            try
            {
                using (var fileStream = new FileStream(zipfile, FileMode.OpenOrCreate))
                {
                    using (var archive = new ZipArchive(fileStream, ZipArchiveMode.Update, true))
                    {
                        var fi = new FileInfo(sourceFile);
                        var data = File.ReadAllBytes(sourceFile);
                        var zipArchiveEntry = archive.CreateEntry(fi.Name, CompressionLevel.Optimal);
                        using (var zipStream = zipArchiveEntry.Open())
                            zipStream.Write(data, 0, data.Length);
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        private string GetServerStatDumpFile(DateTime date)
        {
            try
            {
                date = date.Date;
                var endDate = date.AddDays(1);
                var document = new XmlDocument();
                document.LoadXml("<root></root>");
                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                {
                    var severList = context.Server.ToList();
                    foreach (var server in severList)
                    {
                        var nodeServer = XmlHelper.AddElement(document.DocumentElement, "server");
                        XmlHelper.AddAttribute(nodeServer, "name", server.Name);
                        var list = context.ServerStat.Where(x => date <= x.AddedDate && x.AddedDate < endDate && x.ServerId == server.ServerId).ToList();
                        foreach (var item in list)
                        {
                            var nodeItem = XmlHelper.AddElement(nodeServer, "item");
                            XmlHelper.AddAttribute(nodeItem, "Timestamp", item.AddedDate.ToString("yyyy-MM-dd HH:mm"));
                            XmlHelper.AddAttribute(nodeItem, "MemoryUsageTotal", item.MemoryUsageTotal);
                            XmlHelper.AddAttribute(nodeItem, "MemoryUsageAvailable", item.MemoryUsageAvailable);
                            XmlHelper.AddAttribute(nodeItem, "RepositoryLoadDelta", item.RepositoryLoadDelta);
                            XmlHelper.AddAttribute(nodeItem, "RepositoryUnloadDelta", item.RepositoryUnloadDelta);
                            XmlHelper.AddAttribute(nodeItem, "RepositoryTotal", item.RepositoryTotal);
                            XmlHelper.AddAttribute(nodeItem, "RepositoryCreateDelta", item.RepositoryCreateDelta);
                            XmlHelper.AddAttribute(nodeItem, "RepositoryDeleteDelta", item.RepositoryDeleteDelta);
                            XmlHelper.AddAttribute(nodeItem, "ProcessorUsage", item.ProcessorUsage);
                            XmlHelper.AddAttribute(nodeItem, "MemoryUsageProcess", item.MemoryUsageProcess);
                        }
                    }
                }
                var fileName = Path.Combine(Path.GetTempPath(), date.ToString("yyyy-MM-dd") + ".ServerStat.xml");
                if (File.Exists(fileName))
                {
                    File.Delete(fileName);
                    System.Threading.Thread.Sleep(500);
                }

                document.Save(fileName);
                return fileName;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return null;
            }
        }

        #endregion

        #region Properties

        public int GetRepositoryCount(PagingInfo paging)
        {
            try
            {
                //using (var q = new AcquireReaderLock(Guid.Empty, "GetRepositoryCount"))
                {
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        var lambda = context.Repository.AsQueryable();
                        if (!string.IsNullOrEmpty(paging.Keyword))
                        {
                            lambda = lambda.Where(x =>
                                x.UniqueKey.ToString().Contains(paging.Keyword) ||
                                x.Name.Contains(paging.Keyword));
                        }
                        return lambda.Count();
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public List<BaseRemotingObject> GetRepositoryPropertyList(PagingInfo paging)
        {
            #region Validation

            if (paging.PageOffset < 1)
            {
                throw new Exception("PageOffset must be greater than 0.");
            }

            if (paging.RecordsPerPage < 1)
            {
                throw new Exception("RecordsPerPage must be greater than 0.");
            }

            #endregion

            var timer = Stopwatch.StartNew();
            timer.Start();
            try
            {
                using (var q = new AcquireReaderLock(Guid.Empty, "GetRepositoryPropertyList"))
                {
                    var startIndex = (paging.PageOffset - 1) * paging.RecordsPerPage;

                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        var lambda = context.Repository.AsQueryable();
                        if (!string.IsNullOrEmpty(paging.Keyword))
                        {
                            lambda = lambda.Where(x =>
                                x.UniqueKey.ToString().Contains(paging.Keyword) ||
                                x.Name.Contains(paging.Keyword));
                        }

                        switch ((paging.SortField + string.Empty).ToLower())
                        {
                            case "name":
                                if (paging.SortAsc)
                                    lambda = lambda.OrderBy(x => x.Name);
                                else
                                    lambda = lambda.OrderByDescending(x => x.Name);
                                break;
                            case "id":
                                if (paging.SortAsc)
                                    lambda = lambda.OrderBy(x => x.UniqueKey);
                                else
                                    lambda = lambda.OrderByDescending(x => x.UniqueKey);
                                break;
                            case "hash":
                                if (paging.SortAsc)
                                    lambda = lambda.OrderBy(x => x.VersionHash);
                                else
                                    lambda = lambda.OrderByDescending(x => x.VersionHash);
                                break;
                            case "count":
                                if (paging.SortAsc)
                                    lambda = lambda.OrderBy(x => x.ItemCount);
                                else
                                    lambda = lambda.OrderByDescending(x => x.ItemCount);
                                break;
                            case "created":
                                if (paging.SortAsc)
                                    lambda = lambda.OrderBy(x => x.CreatedDate);
                                else
                                    lambda = lambda.OrderByDescending(x => x.CreatedDate);
                                break;
                            default:
                                lambda = lambda.OrderBy(x => x.Name);
                                break;
                        }

                        var tempList = lambda
                            .Skip(startIndex)
                            .Take(paging.RecordsPerPage)
                            .ToList();

                        paging.TotalItemCount = lambda.Count();

                        var retval = new List<BaseRemotingObject>();
                        tempList.ForEach(x =>
                            retval.Add(new BaseRemotingObject
                            {
                                DataDiskSize = 0,
                                DataMemorySize = 0,
                                IsLoaded = true,
                                ItemCount = x.ItemCount,
                                LastUnloadTime = null,
                                Repository = RepositorySchema.CreateFromXml(x.DefinitionData),
                                VersionHash = x.VersionHash,
                            }));
                        return retval;
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return null;
            }
            finally
            {
                timer.Stop();
                LoggerCQ.LogInfo("GetRepositoryPropertyList: Elapsed=" + timer.ElapsedMilliseconds + ", Keyword='" + paging.Keyword + "'");
            }
        }

        #endregion

        #region Methods

        public void LogRepositoryPerf(RepositorySummmaryStats item)
        {
            try
            {
                StatLogger.Log(item);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public void LogLockStat(LockInfoItem item)
        {
            try
            {
                LockLogger.Log(item);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        private static readonly Dictionary<string, string> _keyCache = new Dictionary<string, string>();

        private bool IsSetupValid()
        {
            var eventLogPermission = new System.Diagnostics.EventLogPermission();
            try
            {
                eventLogPermission.Demand();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError("The service does not have permission to the event log!");
                return false;
            }
            return true;
        }

        public BaseRemotingObject SaveRepository(RepositorySchema repository)
        {
            if (repository == null)
                throw new Exception("Object cannot be null!");

            try
            {
                if (repository.CreatedDate == DateTime.MinValue)
                    repository.CreatedDate = DateTime.Now;

                using (var q = new AcquireWriterLock(repository.ID, "SaveRepository"))
                {
                    if (!RepositoryManager.RepositoryExists(repository.ID))
                    {
                        _createdDelta++;
                    }
                }

                _manager.AddRepository(repository);
                return null;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return null;
            }
        }

        public bool RepositoryExists(Guid repositoryId)
        {
            try
            {
                return RepositoryManager.RepositoryExists(repositoryId);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public void DeleteRepository(RepositorySchema repository)
        {
            try
            {
                if (!RepositoryExists(repository.ID)) return;
                _manager.RemoveRepository(repository.ID);
                using (var q = new AcquireWriterLock(repository.ID, "DeleteRepository"))
                {
                    _deletedDelta++;
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        /// <summary>
        /// Shuts down all repositories
        /// </summary>
        public void ShutDown()
        {
            try
            {
                //if (_timer != null)
                //    _timer.Stop();

                if (_timerStats != null)
                    _timerStats.Stop();

                StatLogger.Shutdown();
                LockLogger.Shutdown();

                _manager.ShutDown();

                //Log Performance counters
                try
                {
                    (new System.Diagnostics.PerformanceCounter(StatLogger.PERFMON_CATEGORY, StatLogger.COUNTER_MEMUSAGE, string.Empty, false)).RawValue = 0;
                    (new System.Diagnostics.PerformanceCounter(StatLogger.PERFMON_CATEGORY, StatLogger.COUNTER_LOADDELTA, string.Empty, false)).RawValue = 0;
                    (new System.Diagnostics.PerformanceCounter(StatLogger.PERFMON_CATEGORY, StatLogger.COUNTER_UNLOADDELTA, string.Empty, false)).RawValue = 0;
                    (new System.Diagnostics.PerformanceCounter(StatLogger.PERFMON_CATEGORY, StatLogger.COUNTER_REPOTOTAL, string.Empty, false)).RawValue = 0;
                    (new System.Diagnostics.PerformanceCounter(StatLogger.PERFMON_CATEGORY, StatLogger.COUNTER_REPOCREATE, string.Empty, false)).RawValue = 0;
                    (new System.Diagnostics.PerformanceCounter(StatLogger.PERFMON_CATEGORY, StatLogger.COUNTER_REPODELETE, string.Empty, false)).RawValue = 0;
                }
                catch (Exception ex)
                {
                    //Do Nothing
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                //throw;
            }
        }

        public void NotifyLoad(Guid repositoryId, int elapsed, int itemsAffected)
        {
            try
            {
                using (var q = new AcquireWriterLock(repositoryId, "NotifyLoad"))
                {
                    if (_loadDelta.ContainsKey(repositoryId))
                        _loadDelta[repositoryId]++;
                    else
                        _loadDelta.Add(repositoryId, 1);
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public void NotifyUnload(Guid repositoryId, int elapsed, int itemsAffected)
        {
            try
            {
                using (var q = new AcquireWriterLock(repositoryId, "NotifyUnload"))
                {
                    if (_unloadDelta.ContainsKey(repositoryId))
                        _unloadDelta[repositoryId]++;
                    else
                        _unloadDelta.Add(repositoryId, 1);
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public RealtimeStats[] PerformanceCounters(DateTime minDate, DateTime maxDate)
        {
            try
            {
                using (var q = new AcquireReaderLock(Guid.Empty, "PerformanceCounters"))
                {
                    return StatLogger.QueryServerStats(minDate, maxDate).ToArray();
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public RepositorySummmaryStats GetRepositoryStats(Guid repositoryId, DateTime minDate, DateTime maxDate)
        {
            try
            {
                using (var q = new AcquireReaderLock(repositoryId, "GetRepositoryStats"))
                {
                    return StatLogger.QueryRepositoryStats(repositoryId, minDate, maxDate);
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public SystemStats GetSystemStats()
        {
            try
            {
                var info = new PerformanceInformation();
                GetPerformanceInfo(out info, Marshal.SizeOf(info));
                var retval = new SystemStats()
                {
                    MachineName = Environment.MachineName,
                    OSVersion = Environment.OSVersion.ToString(),
                    ProcessorCount = Environment.ProcessorCount,
                    TickCount = Environment.TickCount,
                    TotalMemory = info.PhysicalTotal.ToInt64() * info.PageSize.ToInt64(),
                };

                retval.RepositoryCount = GetRepositoryCount(new PagingInfo());
                return retval;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public bool IsSystemReady()
        {
            return _isSystemReady;
        }

        #endregion

        void IDisposable.Dispose()
        {
            this.ShutDown();
        }

    }

}