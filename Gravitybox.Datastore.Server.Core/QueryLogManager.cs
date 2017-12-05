#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;

namespace Gravitybox.Datastore.Server.Core
{
    internal class QueryLogManager
    {
        private List<Gravitybox.Datastore.EFDAL.Entity.RepositoryLog> _cache = new List<EFDAL.Entity.RepositoryLog>();
        private System.Timers.Timer _timer = null;
        private readonly Guid QueryLogID = new Guid("1DDA6910-F4F6-477E-B9AC-B10E59C4BD63");

        public QueryLogManager()
        {
            //Every 5 minutes perform housekeeping
            _timer = new System.Timers.Timer(10000);
            _timer.Elapsed += _timer_Elapsed;
            _timer.Start();
        }

        public void Log(Gravitybox.Datastore.EFDAL.Entity.RepositoryLog logItem)
        {
            using (var q = new AcquireWriterLock(QueryLogID, "QueryLog"))
            {
                _cache.Add(logItem);
            }
        }

        public void Empty()
        {
            _timer_Elapsed(null, null);
        }

        private void _timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            List<Gravitybox.Datastore.EFDAL.Entity.RepositoryLog> work;
            try
            {
                //Copy the cache list and empty so other threads can continue to use it
                using (var q = new AcquireReaderLock(QueryLogID, "QueryLog"))
                {
                    work = _cache.ToList();
                    _cache.Clear();
                }

                //Save all of these to disk
                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                {
                    foreach (var item in work)
                    {
                        context.AddItem(item);
                    }
                    context.SaveChanges();
                }
            }
            catch (Exception ex)
            {
                //LoggerCQ.LogError(ex);
                //throw;
            }
        }

    }
}
