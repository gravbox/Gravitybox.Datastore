using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;
using Gravitybox.Datastore.EFDAL.Entity;
using Gravitybox.Datastore.Server.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.Housekeeping
{
    //Process any house keeping items in queue
    internal class HousekeepingMonitor
    {
        private System.Timers.Timer _timer = null;

        public HousekeepingMonitor()
        {
            _timer = new System.Timers.Timer(60000);
            _timer.Elapsed += _timer_Elapsed;
            _timer.Start();
        }

        private void _timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            try
            {
                _timer.Stop();
                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                {
                    var list = context.Housekeeping.ToList();
                    foreach (var item in list)
                    {
                        if (item.TypeValue == HousekeepingTaskType.ClearRepositoryLog)
                        {
                            var obj = ServerUtilities.DeserializeObject<HkClearRepositoryLog>(item.Data);
                            if (obj.Run())
                            {
                                context.DeleteItem(item);
                                context.SaveChanges();
                            }
                        }
                        else
                        {
                            LoggerCQ.LogWarning("Unknown housekeeping type: " + item.Type);
                        }
                    }
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

        public void QueueTask(IHousekeepingTask task)
        {
            try
            {
                if (task == null) return;
                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                {
                    var newItem = new EFDAL.Entity.Housekeeping
                    {
                        Data = ServerUtilities.SerializeObject(task),
                        Type = (int)task.Type,
                    };
                    context.AddItem(newItem);
                    context.SaveChanges();
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }
    }
}
