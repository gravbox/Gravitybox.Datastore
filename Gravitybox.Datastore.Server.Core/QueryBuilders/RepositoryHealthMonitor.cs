using Gravitybox.Datastore.Common;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;

namespace Gravitybox.Datastore.Server.Core.QueryBuilders
{
    internal static class RepositoryHealthMonitor
    {
        private static readonly System.Timers.Timer _timer = null;
        private static readonly ConcurrentBag<Guid> _repositoryList = new ConcurrentBag<Guid>();

        private static int _isProcessing = 0;

        static RepositoryHealthMonitor()
        {
#if DEBUG
            const int TimeInterval = 5000;  // reduce interval to 5 seconds for easier testing
#else
            const int TimeInterval = 10 * 60 * 1000;  // 10 minutes
#endif

            _timer = new System.Timers.Timer(TimeInterval);
            _timer.Elapsed += _timer_Elapsed;
            _timer.Start();

        }

        public static bool IsActive { get; set; } = true;

        private static void _timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            // If the system is turned off then do nothing
            if (!IsActive) return;

            // Test for timer reentry
            var isProcessing = Interlocked.Exchange(ref _isProcessing, 1);
            if (isProcessing != 0)
            {
                LoggerCQ.LogTrace("RepositoryHealthMonitor: timer event already active");
                return;
            }

            try
            {
                if (_repositoryList.Any())
                {
                    using (new PerformanceLogger($"ProcessHealthChecks: Count={_repositoryList.Count}"))
                    {
                        ProcessHealthChecks();
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, "RepositoryHealthMonitor: timer event failed");
            }
            finally
            {
                Interlocked.Exchange(ref _isProcessing, 0);  // clear the processing flag
            }
        }

        private static void ProcessHealthChecks()
        {
            var manager = ((SystemCore) RepositoryManager.SystemCore).Manager;
            while (_repositoryList.TryTake(out var id))
            {
                if (!IsActive) return; // Stop when housekeeping comes on

                LoggerCQ.LogDebug($"ProcessHealthChecks: RepositoryId={id}");
                var schema = RepositoryManager.GetSchema(id);
                if (schema != null)
                {
                    manager.UpdateSchema(schema, true);
                }
            }
        }

        public static void HealthCheck(Guid id)
        {
            lock (_repositoryList)
            {
                if (!_repositoryList.Contains(id))
                {
                    _repositoryList.Add(id);
                    LoggerCQ.LogTrace($"HealthCheck queued: RepositoryId={id}");
                }
            }
        }
    }
}