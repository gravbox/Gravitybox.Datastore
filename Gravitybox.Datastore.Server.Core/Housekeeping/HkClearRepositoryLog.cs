using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL.Entity;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Server.Core.Housekeeping
{
    [Serializable]
    public class HkClearRepositoryLog : IHousekeepingTask
    {
        public HousekeepingTaskType Type => HousekeepingTaskType.ClearRepositoryLog;

        public int RepositoryId { get; set; }

        public DateTime? PivotDate { get; set; }

        public bool Run()
        {
            var timer = Stopwatch.StartNew();
            try
            {
                var sb = new StringBuilder();
                sb.AppendLine("SET ROWCOUNT 5000");
                if (this.PivotDate == null)
                    sb.AppendLine("DELETE FROM [RepositoryLog] WHERE [RepositoryId] = " + this.RepositoryId);
                else
                    sb.AppendLine("DELETE FROM [RepositoryLog] WHERE [RepositoryId] = " + this.RepositoryId + " AND [CreatedDate] <= '" + this.PivotDate.Value.ToString("yyyy-MM-dd HH:mm:ss") + "'");

                var count = 0;
                var tempCount = 0;
                do
                {
                    RetryHelper.DefaultRetryPolicy(5)
                        .Execute(() =>
                        {
                            tempCount = SqlHelper.ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), null, false);
                            count += tempCount;
                        });
                }
                while (tempCount > 0);
                timer.Stop();
                LoggerCQ.LogInfo("HkClearRepositoryLog: Count=" + count + ", RepositoryId=" + this.RepositoryId + ", Elapsed=" + timer.ElapsedMilliseconds);
                return true;
            }
            catch (Exception ex)
            {
                timer.Stop();
                LoggerCQ.LogWarning(ex);
                return false;
            }
        }
    }
}
