#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;
using Gravitybox.Datastore.EFDAL.Entity;
using Gravitybox.Datastore.Server.Interfaces;

namespace Gravitybox.Datastore.Server.Core
{
    internal static class LockLogger
    {
        private const int LOG_TIMER_INTERVAL = 30 * 1000;
        private static bool _ready = false;
        private static DateTime _pivot = new DateTime(2000, 1, 1);
        private static List<LockInfoItem> _cache = new List<LockInfoItem>();
        private static System.Timers.Timer _timer = null;

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

        #region Repository Stats

        private static void TimerTick(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (!_ready) return;

            _timer.Stop();
            try
            {
                //Lock the stats list and build queries
                List<LockInfoItem> copyCache = null;
                lock (_cache)
                {
                    copyCache = _cache.ToList();
                    _cache.Clear();
                }

                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                {
                    foreach (var item in copyCache)
                    {
                        var newItem = new LockStat()
                                      {
                                          CurrentReadCount = item.CurrentReadCount,
                                          Elapsed = item.Elapsed,
                                          Failure = item.Failure,
                                          IsWriteLockHeld = item.IsWriteLockHeld,
                                          ThreadId = item.ThreadId,
                                          WaitingReadCount = item.WaitingReadCount,
                                          WaitingWriteCount = item.WaitingWriteCount,
                                          DateStamp = item.DateStamp,
                                          TraceInfo = item.TraceInfo,
                                      };
                        context.AddItem(newItem);
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

        public static void Log(LockInfoItem item)
        {
            try
            {
                if (!ConfigHelper.AllowLockStats) return;
                lock (_cache)
                {
                    _cache.Add(item);
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        #endregion
    }
}
