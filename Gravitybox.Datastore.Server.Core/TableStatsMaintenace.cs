using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core
{
    internal class TableStatsMaintenace
    {
        private static HashSet<Guid> _statisticsCache = new HashSet<Guid>();
        public const int StatCheckThreshold = 20000;
        private System.Timers.Timer _timerStats = null;
        private DateTime _lastTimerStats = DateTime.Now;

        public TableStatsMaintenace(bool enableHouseKeeping)
        {
            if (enableHouseKeeping)
            {
                _timerStats = new System.Timers.Timer(60 * 1000 * 15); //15 minutes
                _timerStats.Elapsed += _timerStats_Elapsed;
                _timerStats.Start();
            }
        }

        public void MarkRefreshStats(Guid repositoryId)
        {
            lock (_statisticsCache)
            {
                if (!_statisticsCache.Contains(repositoryId))
                {
                    _statisticsCache.Add(repositoryId);
                    LoggerCQ.LogInfo($"TableStatsMaintenace MarkRefreshStats: ID={repositoryId}");
                }
            }
        }

        private void _timerStats_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            try
            {
                if (DateTime.Now.Subtract(_lastTimerStats).TotalHours <= 12) return;
                _lastTimerStats = DateTime.Now;

                HashSet<Guid> copy = null;
                lock (_statisticsCache)
                {
                    copy = new HashSet<Guid>(_statisticsCache);
                    _statisticsCache.Clear();
                }

                var timer = Stopwatch.StartNew();
                foreach (var g in copy)
                {
                    SqlHelper.UpdateStatistics(g);
                }
                timer.Stop();

                if (copy.Count > 0)
                    LoggerCQ.LogDebug($"Update table statistics: Count={copy.Count}, Elapsed={timer.ElapsedMilliseconds}");
            }
            catch (Exception ex)
            {
                LoggerCQ.LogWarning(ex);
            }
        }

    }
}
