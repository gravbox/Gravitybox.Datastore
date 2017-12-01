using Gravitybox.Datastore.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.QueryBuilders
{
    internal static class RepositoryHealthMonitor
    {
        private static System.Timers.Timer _timer = null;
        private static ConcurrentBag<Guid> _repositoryList = new ConcurrentBag<Guid>();

        static RepositoryHealthMonitor()
        {
#if DEBUG
            const int TimeInterval = 5000;
#else
            const int TimeInterval = 60000;
#endif

            _timer = new System.Timers.Timer(TimeInterval);
            _timer.Elapsed += _timer_Elapsed;
            _timer.Start();

        }

        public static bool IsActive { get; set; } = true;

        private static void _timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            //If the system is turned off then do nothing
            if (!IsActive) return;

            try
            {
                Guid id;
                var manager = ((SystemCore)RepositoryManager.SystemCore).Manager;
                while (_repositoryList.TryTake(out id))
                {
                    if (!IsActive) return; //Stop when housekeeping comes on

                    var schema = RepositoryManager.GetSchema(id);
                    if (schema != null)
                    {
                        manager.UpdateSchema(schema, true);
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void HealthCheck(Guid id)
        {
            lock (_repositoryList)
            {
                if (!_repositoryList.Contains(id))
                    _repositoryList.Add(id);
            }
        }

    }
}