using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;
using Gravitybox.Datastore.EFDAL.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.Housekeeping
{
    //Process any house keeping items in queue
    internal class HousekeepingMonitor
    {
        private System.Timers.Timer _timer = null;
        private bool _isProcessing = false;

        public HousekeepingMonitor()
        {
#if DEBUG
            _timer = new System.Timers.Timer(10000);
#else
            _timer = new System.Timers.Timer(60000);
#endif
            _timer.Elapsed += _timer_Elapsed;
            _timer.Start();
        }

        private void _timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (_isProcessing) return;
            _timer.Stop();
            try
            {
                _isProcessing = true;
                this.DoHouseKeeping();
                this.ProcessDeleted();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex.Message);
            }
            finally
            {
                _timer.Start();
                _isProcessing = false;
            }
        }

        private void DoHouseKeeping()
        {
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
                        LoggerCQ.LogWarning($"Unknown housekeeping type: {item.Type}");
                    }
                }
            }
        }

        private void ProcessDeleted()
        {
            try
            {
                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                {
                    const int CHUNKSIZE = 20000;
                    var deleteList = context.DeleteQueue.Where(x => x.IsReady).ToList();
                    foreach (var dItem in deleteList)
                    {
                        var rKey = context.Repository.Where(x => x.RepositoryId == dItem.RepositoryId).Select(x => x.UniqueKey).FirstOrDefault();
                        if (rKey != Guid.Empty)
                        {
                            var schema = RepositoryManager.GetSchema(rKey);
                            if (schema != null)
                            {
                                #region Parent Schema
                                RepositorySchema parentSchema = null;
                                if (schema.ParentID != null)
                                {
                                    if (string.IsNullOrEmpty(schema.ObjectAlias))
                                        throw new Exception("An inherited repository must have an alias.");

                                    parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value, true);
                                    if (parentSchema == null)
                                        throw new Exception("Parent schema not found");

                                    if (!context.Repository.Any(x => x.UniqueKey == schema.ParentID && x.ParentId == null))
                                        throw new Exception("Cannot create an repository from a non-base parent");
                                    schema = parentSchema.Merge(schema);
                                }
                                #endregion

                                var listDimensions = schema.FieldList
                                    .Where(x => x.DataType == RepositorySchema.DataTypeConstants.List &&
                                        x is DimensionDefinition)
                                        .Cast<DimensionDefinition>()
                                    .ToList();

                                foreach (var dimension in listDimensions)
                                {
                                    var timer = Stopwatch.StartNew();
                                    var listTable = SqlHelper.GetListTableName(schema.ID, dimension.DIdx);
                                    if (parentSchema != null && parentSchema.DimensionList.Any(x => x.DIdx == dimension.DIdx))
                                        listTable = SqlHelper.GetListTableName(schema.ParentID.Value, dimension.DIdx);

                                    var newParam = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = $"@ParentRowId", Value = dItem.RowId };

                                    var sbList = new StringBuilder();
                                    sbList.AppendLine($"--MARKER 19");
                                    sbList.AppendLine($"SET ROWCOUNT {CHUNKSIZE};");
                                    sbList.AppendLine("set nocount off;");
                                    sbList.AppendLine($"WITH S([{SqlHelper.RecordIdxField}])");
                                    sbList.AppendLine("AS");
                                    sbList.AppendLine("(");
                                    sbList.AppendLine($"select [RecordIdx] from [DeleteQueueItem] {SqlHelper.NoLockText()}");
                                    sbList.AppendLine($"where [ParentRowId] = {newParam.ParameterName}");
                                    sbList.AppendLine(")");
                                    sbList.AppendLine($"DELETE FROM [{listTable}]");
                                    sbList.AppendLine($"FROM S inner join [{listTable}] on S.[{SqlHelper.RecordIdxField}] = [{listTable}].[{SqlHelper.RecordIdxField}];");
                                    var lastCount = 0;
                                    do
                                    {
                                        lastCount = SqlHelper.ExecuteSql(ConfigHelper.ConnectionString, sbList.ToString(), new[] { newParam });
                                    } while (lastCount >= CHUNKSIZE);

                                    timer.Stop();
                                } //Dimension

                            }
                        }

                        //Remove from queue
                        context.DeleteQueueItem.Where(x => x.ParentRowId == dItem.RowId).Delete();
                        context.DeleteQueue.Where(x => x.RowId == dItem.RowId).Delete();
                        context.SaveChanges();
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
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