#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;

namespace Gravitybox.Datastore.Server.Core
{
    internal class DimensionCache
    {
        private const int MAXITEMS = 500; //Number of cache items to keep
        private List<CacheResults> _cache = new List<CacheResults>();
        private Dictionary<Guid, RepositorySchema> _parentSchemaCache = new Dictionary<Guid, RepositorySchema>();
        private System.Timers.Timer _timer = null;

        public DimensionCache()
        {
            try
            {
                //Cull cache every minute
                _timer = new System.Timers.Timer(60000);
                _timer.Elapsed += _timer_Elapsed;
                _timer.Start();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                //throw;
            }
        }

        private void _timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            _timer.Stop();
            try
            {
                lock (_cache)
                {
                    //Keep only the last N items
                    var l = _cache
                        .OrderByDescending(x => x.Timestamp)
                        .Skip(MAXITEMS)
                        .ToList();
                    l.ForEach(x => _cache.Remove(x));

                    //Purge anything not used in an hour
                    l.Where(x => DateTime.Now.Subtract(x.Timestamp).TotalMinutes >= 60)
                        .ToList()
                        .ForEach(x => _cache.Remove(x));
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogWarning(ex, "DimensionCache housekeeping failed");
            }
            finally
            {
                _timer.Start();
            }
        }

        private CacheResults GetCache(DatastoreEntities context, int id, RepositorySchema schema)
        {
            try
            {
                var dimensionTableName = SqlHelper.GetDimensionTableName(schema.ID);
                var dimensionValueTableName = SqlHelper.GetDimensionValueTableName(schema.ID);
                var dimensionTableNameParent = string.Empty;
                var dimensionValueTableNameParent = string.Empty;

                lock (_cache)
                {
                    var dimensionStamp = RepositoryManager.GetDimensionChanged(context, id);
                    var retval = _cache.FirstOrDefault(x => x.RepositoryId == id);

                    //Check repository DimensionStamp and if changed the reload dimensions
                    if (retval != null && retval.DimensionStamp != dimensionStamp)
                    {
                        Clear(id);
                        retval = null;
                    }

                    if (retval == null)
                    {
                        #region Parent table stuff
                        if (schema.ParentID != null)
                        {
                            if (!_parentSchemaCache.ContainsKey(schema.ID))
                            {
                                var parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);
                                _parentSchemaCache.Add(schema.ID, schema.Subtract(parentSchema));
                            }
                            dimensionTableNameParent = SqlHelper.GetDimensionTableName(schema.ParentID.Value);
                            dimensionValueTableNameParent = SqlHelper.GetDimensionValueTableName(schema.ParentID.Value);
                        }
                        #endregion

                        retval = new CacheResults() { RepositoryId = id, ParentId = RepositoryManager.GetSchemaParentId(id) };
                        _cache.Add(retval);

                        var sb = new StringBuilder();
                        sb.AppendLine("select d.DIdx, v.DVIdx, v.Value from [" + dimensionTableName + "] d left join [" + dimensionValueTableName + "] v on D.DIdx = v.DIdx");
                        
                        //If there is a parent schema then UNION its dimension tables
                        if (schema.ParentID != null)
                            sb.AppendLine("union select d.DIdx, v.DVIdx, v.Value from [" + dimensionTableNameParent + "] d left join [" + dimensionValueTableNameParent + "] v on D.DIdx = v.DIdx");

                        sb.AppendLine("order by DIdx, DVIdx");

                        var ds = SqlHelper.GetDataset(ConfigHelper.ConnectionString, sb.ToString(), null);
                        retval.Results = new List<DimensionItem>();
                        foreach (DataRow dr in ds.Tables[0].Rows)
                        {
                            var didx = (long)dr["DIdx"];
                            long dvidx = 0;
                            string v = null;
                            if (dr["DVIdx"] != System.DBNull.Value)
                            {
                                dvidx = (long)dr["DVIdx"];
                                v = (string)dr["Value"];
                            }
                            var d = retval.Results.FirstOrDefault(x => x.DIdx == didx);
                            if (d == null)
                            {
                                d = new DimensionItem { DIdx = (int)didx, Name = schema.DimensionList.Where(x => x.DIdx == didx).Select(x => x.Name).FirstOrDefault() };
                                retval.Results.Add(d);
                            }
                            if (dvidx != 0)
                                d.RefinementList.Add(new RefinementItem { DVIdx = dvidx, FieldValue = v, DIdx = didx });

                            //Rearrange all refinements alpha (for debugging and such)
                            //retval.Results.ForEach(ditem => ditem.RefinementList = ditem.RefinementList.OrderBy(x => x.FieldValue).ToList());
                        }
                    }
                    retval.DimensionStamp = dimensionStamp;
                    retval.Timestamp = DateTime.Now; //Accessed
                    return retval;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public void Clear(int id)
        {
            lock (_cache)
            {
                _cache.RemoveAll(x => x.RepositoryId == id);
                _cache.RemoveAll(x => x.ParentId == id);
            }
        }

        public List<DimensionItem> Get(DatastoreEntities context, RepositorySchema schema, int id, IEnumerable<DataItem> list = null)
        {
            if (schema == null)
                throw new Exception("The schema is null");

            try
            {
                var dimensionTableName = SqlHelper.GetDimensionTableName(schema.ID);
                var dimensionValueTableName = SqlHelper.GetDimensionValueTableName(schema.ID);
                var dimensionTableNameParent = string.Empty;
                var dimensionValueTableNameParent = string.Empty;
                var saveCount = 0;

                lock (_cache)
                {
                    var retval = GetCache(context, id, schema);

                    #region Do this after "GetCache" call as it will flush the cache if need be
                    //If there is a parent repository then get parent schema as will will need to know which dimension table to use for different fields
                    RepositorySchema diff = null;
                    if (schema.ParentID != null)
                    {
                        if (!_parentSchemaCache.ContainsKey(schema.ID))
                        {
                            var parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);
                            _parentSchemaCache.Add(schema.ID, schema.Subtract(parentSchema));
                        }
                        diff = _parentSchemaCache[schema.ID];
                        dimensionTableNameParent = SqlHelper.GetDimensionTableName(schema.ParentID.Value);
                        dimensionValueTableNameParent = SqlHelper.GetDimensionValueTableName(schema.ParentID.Value);
                    }
                    #endregion

                    #region Create the dimensions if need be
                    {
                        var needSave = false;
                        if (diff == null)
                        {
                            //This is for stand-alone tables. There is only one dimension table
                            var sb = new StringBuilder();
                            var parameters = new List<SqlParameter>();
                            var didxParam = 0;
                            foreach (var dimensionDef in schema.DimensionList)
                            {
                                if (!retval.Results.Any(x => x.DIdx == dimensionDef.DIdx))
                                {
                                    var param = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@__didx" + didxParam, Value = dimensionDef.DIdx };
                                    parameters.Add(param);
                                    sb.AppendLine("insert into [" + dimensionTableName + "] (DIdx) values (" + param.ParameterName + ")");
                                    didxParam++;
                                    needSave = true;
                                }
                            }
                            if (needSave)
                                SqlHelper.ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), parameters, false);
                        }
                        else
                        {
                            //This is for inherited tables. Figure out which dimension table to use
                            var sb = new StringBuilder();
                            var parameters = new List<SqlParameter>();
                            var didxParam = 0;
                            foreach (var dimensionDef in schema.DimensionList)
                            {
                                var tempTable = dimensionTableNameParent;
                                if (diff.DimensionList.Any(x => x.DIdx == dimensionDef.DIdx))
                                    tempTable = dimensionTableName;

                                if (!retval.Results.Any(x => x.DIdx == dimensionDef.DIdx))
                                {
                                    var param = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@__didx" + didxParam, Value = dimensionDef.DIdx };
                                    parameters.Add(param);
                                    sb.AppendLine("insert into [" + tempTable + "] (DIdx) values (" + param.ParameterName + ")");
                                    didxParam++;
                                    needSave = true;
                                }
                            }
                            if (needSave)
                                SqlHelper.ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), parameters, false);
                        }

                        #region Save so far
                        if (needSave)
                        {
                            Clear(id);
                            SqlHelper.MarkDimensionsChanged(id);
                            retval = GetCache(context, id, schema);
                            needSave = false;
                        }
                        #endregion
                    }
                    #endregion

                    #region Find new refinements in list

                    //Create a cache of all next keys
                    var _nextKeys = new Dictionary<DimensionItem, long>();
                    retval.Results.ForEach(z => _nextKeys.Add(z, z.RefinementList.OrderByDescending(x => x.DVIdx).Select(x => x.DVIdx).FirstOrDefault() + 1));

                    if (list != null)
                    {
                        var index = 0;
                        foreach (var field in schema.FieldList)
                        {
                            var values = new HashSet<string>();
                            if (field is DimensionDefinition)
                            {
                                var dimension = field as DimensionDefinition;

                                //for this dimension find unique values
                                foreach (var item in list)
                                {
                                    if (index < item.ItemArray.Length)
                                    {
                                        if (item.ItemArray[index] != null)
                                        {
                                            if (field.DataType == RepositorySchema.DataTypeConstants.List)
                                            {
                                                var l = (string[])item.ItemArray[index];
                                                foreach (var v in l)
                                                {
                                                    if (!values.Contains(v))
                                                        values.Add(v);
                                                }
                                            }
                                            else
                                            {
                                                if ((dimension.DataType == RepositorySchema.DataTypeConstants.Int || dimension.DataType == RepositorySchema.DataTypeConstants.Int64) && dimension.NumericBreak != null && dimension.NumericBreak > 0)
                                                {
                                                    var v = Convert.ToInt64(item.ItemArray[index]);
                                                    var scaled = ((v / dimension.NumericBreak) * dimension.NumericBreak).ToString();
                                                    if (!values.Contains(scaled))
                                                        values.Add(scaled);
                                                }
                                                else
                                                {
                                                    var v = SqlHelper.GetTypedDimValue(field.DataType, item.ItemArray[index]);
                                                    if (!values.Contains(v))
                                                        values.Add(v);
                                                }
                                            }
                                        }
                                    }
                                }

                                //for unique values if not exist then insert
                                foreach (var v in values)
                                {
                                    var needSave = false;
                                    var paramIndex = 0;
                                    var didxParam = 0;
                                    var dvidxParam = 0;
                                    var sb = new StringBuilder();
                                    var parameters = new List<SqlParameter>();

                                    long baseDVIdx;
                                    if (schema.ParentID != null && diff.DimensionList.Any(x => x.DIdx == dimension.DIdx))
                                        baseDVIdx = ((dimension.DIdx - Constants.DGROUPEXT) + 1) * Constants.DVALUEGROUPEXT; //Child Repository
                                    else
                                        baseDVIdx = ((dimension.DIdx - Constants.DGROUP) + 1) * Constants.DVALUEGROUP; //Normal

                                    var dbDimension = retval.Results.FirstOrDefault(x => x.DIdx == dimension.DIdx);
                                    if (!dbDimension.RefinementList.Any(x => x.FieldValue == v))
                                    {
                                        if (!_nextKeys.ContainsKey(dbDimension)) //If was empty then default to base index
                                            _nextKeys.Add(dbDimension, baseDVIdx);
                                        if (_nextKeys[dbDimension] == 1) //If was empty then default to base index
                                            _nextKeys[dbDimension] = baseDVIdx;

                                        var nextDVIdx = _nextKeys[dbDimension];
                                        _nextKeys[dbDimension]++;

                                        var newParam = new SqlParameter { DbType = DbType.String, IsNullable = false, ParameterName = "@__z" + paramIndex, Value = v };
                                        parameters.Add(newParam);
                                        paramIndex++;

                                        if (diff == null)
                                        {
                                            //This is for stand-alone tables. There is only one dimension table
                                            var paramDIdx = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@__didx" + didxParam, Value = dimension.DIdx };
                                            parameters.Add(paramDIdx);
                                            var paramDVIdx = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@__dvidx" + dvidxParam, Value = nextDVIdx };
                                            parameters.Add(paramDVIdx);
                                            didxParam++;
                                            dvidxParam++;

                                            sb.AppendLine("if not exists(select * from [" + dimensionValueTableName + "] where [DIdx] = " + paramDIdx.ParameterName + " and [DVIdx] = " + paramDVIdx.ParameterName + ")");
                                            sb.AppendLine("insert into [" + dimensionValueTableName + "] ([DIdx], [DVIdx], [Value]) values (" + paramDIdx.ParameterName + ", " + paramDVIdx.ParameterName + ", " + newParam.ParameterName + ")");
                                        }
                                        else
                                        {
                                            //This is for inherited tables. Figure out which dimension table to use
                                            var tempTable = dimensionValueTableNameParent;
                                            if (diff.DimensionList.Any(x => x.DIdx == dimension.DIdx))
                                                tempTable = dimensionValueTableName;

                                            var paramDIdx = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@__didx" + didxParam, Value = dimension.DIdx };
                                            parameters.Add(paramDIdx);
                                            var paramDVIdx = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@__dvidx" + dvidxParam, Value = nextDVIdx };
                                            parameters.Add(paramDVIdx);
                                            didxParam++;
                                            dvidxParam++;

                                            sb.AppendLine("if not exists(select * from [" + tempTable + "] where [DIdx] = " + paramDIdx.ParameterName + " and [DVIdx] = " + paramDVIdx.ParameterName + ")");
                                            sb.AppendLine("insert into [" + tempTable + "] ([DIdx], [DVIdx], [Value]) values (" + paramDIdx.ParameterName + ", " + paramDVIdx.ParameterName + ", " + newParam.ParameterName + ")");
                                        }
                                        needSave = true;
                                    }
                                    if (needSave)
                                    {
                                        SqlHelper.ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), parameters, false);
                                        saveCount++;
                                    }
                                }
                            }
                            index++;
                        }

                    }
                    #endregion

                    if (saveCount > 0)
                    {
                        Clear(id);
                        SqlHelper.MarkDimensionsChanged(id);
                        retval = GetCache(context, id, schema);
                    }

                    return retval.Results;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public List<DimensionItem> Get(DatastoreEntities context, RepositorySchema schema, int id, IEnumerable<DataFieldUpdate> list = null)
        {
            if (schema == null)
                throw new Exception("The schema is null");

            try
            {
                var dimensionTableName = SqlHelper.GetDimensionTableName(schema.ID);
                var dimensionValueTableName = SqlHelper.GetDimensionValueTableName(schema.ID);
                var dimensionTableNameParent = string.Empty;
                var dimensionValueTableNameParent = string.Empty;
                var parameters = new List<SqlParameter>();
                var didxParam = 0;

                lock (_cache)
                {
                    var retval = GetCache(context, id, schema);
                    var needSave = false;

                    #region Do this after "GetCache" call as it will flush the cache if need be
                    //If there is a parent repository then get parent schema as will will need to know which dimension table to use for different fields
                    RepositorySchema diff = null;
                    if (schema.ParentID != null)
                    {
                        if (!_parentSchemaCache.ContainsKey(schema.ID))
                        {
                            var parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);
                            _parentSchemaCache.Add(schema.ID, schema.Subtract(parentSchema));
                        }
                        diff = _parentSchemaCache[schema.ID];
                        dimensionTableNameParent = SqlHelper.GetDimensionTableName(schema.ParentID.Value);
                        dimensionValueTableNameParent = SqlHelper.GetDimensionValueTableName(schema.ParentID.Value);
                    }
                    #endregion

                    #region Create the dimensions if need be
                    var sb = new StringBuilder();
                    if (diff == null)
                    {
                        //This is for stand-alone tables. There is only one dimension table
                        foreach (var dimensionDef in schema.DimensionList)
                        {
                            if (!retval.Results.Any(x => x.DIdx == dimensionDef.DIdx))
                            {
                                var param = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@__didx" + didxParam, Value = dimensionDef.DIdx };
                                parameters.Add(param);
                                sb.AppendLine("insert into [" + dimensionTableName + "] (DIdx) values (" + param.ParameterName + ")");
                                didxParam++;
                                needSave = true;
                            }
                        }
                    }
                    else
                    {
                        //This is for inherited tables. Figure out which dimension table to use
                        foreach (var dimensionDef in schema.DimensionList)
                        {
                            var tempTable = dimensionTableNameParent;
                            if (diff.DimensionList.Any(x => x.DIdx == dimensionDef.DIdx))
                                tempTable = dimensionTableName;

                            if (!retval.Results.Any(x => x.DIdx == dimensionDef.DIdx))
                            {
                                var param = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@__didx" + didxParam, Value = dimensionDef.DIdx };
                                parameters.Add(param);
                                sb.AppendLine("insert into [" + tempTable + "] (DIdx) values (" + param.ParameterName + ")");
                                didxParam++;
                                needSave = true;
                            }
                        }
                    }
                    #endregion

                    #region Save so far
                    if (needSave)
                    {
                        SqlHelper.ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), parameters, false);
                        Clear(id);
                        SqlHelper.MarkDimensionsChanged(id);
                        retval = GetCache(context, id, schema);
                        needSave = false;
                    }
                    #endregion
                    sb = new StringBuilder();
                    parameters = new List<SqlParameter>();
                    didxParam = 0;
                    var dvidxParam = 0;

                    #region Find new refinements in list

                    //Create a cache of all next keys
                    var _nextKeys = new Dictionary<DimensionItem, long>();
                    //TODO: this is taking too long on every request (~1%)
                    retval.Results.ForEach(z => _nextKeys.Add(z, z.RefinementList.OrderByDescending(x => x.DVIdx).Select(x => x.DVIdx).FirstOrDefault() + 1));

                    var paramIndex = 0;
                    if (list != null)
                    {
                        foreach (var item in list.Where(x => x.FieldValue != null))
                        {
                            var values = new HashSet<string>();
                            var dimension = schema.FieldList.FirstOrDefault(x => x.Name == item.FieldName) as DimensionDefinition;
                            if (dimension != null)
                            {
                                if (dimension.DataType == RepositorySchema.DataTypeConstants.List)
                                {
                                    var l = (string[])item.FieldValue;
                                    foreach (var v in l)
                                    {
                                        if (!values.Contains(v))
                                            values.Add(v);
                                    }
                                }
                                else
                                {
                                    if ((dimension.DataType == RepositorySchema.DataTypeConstants.Int || dimension.DataType == RepositorySchema.DataTypeConstants.Int64) && dimension.NumericBreak != null && dimension.NumericBreak > 0)
                                    {
                                        var v = Convert.ToInt64(item.FieldValue);
                                        var scaled = ((v / dimension.NumericBreak) * dimension.NumericBreak).ToString();
                                        if (!values.Contains(scaled))
                                            values.Add(scaled);
                                    }
                                    else
                                    {
                                        var v = SqlHelper.GetTypedDimValue(dimension.DataType, item.FieldValue);
                                        if (!values.Contains(v))
                                            values.Add(v);
                                    }
                                }
                            }


                            //for unique values if not exist then insert
                            foreach (var v in values)
                            {
                                long baseDVIdx;
                                if (schema.ParentID != null && diff.DimensionList.Any(x => x.DIdx == dimension.DIdx))
                                    baseDVIdx = ((dimension.DIdx - Constants.DGROUPEXT) + 1) * Constants.DVALUEGROUPEXT; //Child Repository
                                else
                                    baseDVIdx = ((dimension.DIdx - Constants.DGROUP) + 1) * Constants.DVALUEGROUP; //Normal

                                var dbDimension = retval.Results.FirstOrDefault(x => x.DIdx == dimension.DIdx);
                                if (!dbDimension.RefinementList.Any(x => x.FieldValue == v))
                                {
                                    if (!_nextKeys.ContainsKey(dbDimension)) //If was empty then default to base index
                                        _nextKeys.Add(dbDimension, baseDVIdx);
                                    if (_nextKeys[dbDimension] == 1) //If was empty then default to base index
                                        _nextKeys[dbDimension] = baseDVIdx;

                                    var nextDVIdx = _nextKeys[dbDimension];
                                    _nextKeys[dbDimension]++;

                                    var newParam = new SqlParameter { DbType = DbType.String, IsNullable = false, ParameterName = $"@__z{paramIndex}", Value = v };
                                    parameters.Add(newParam);
                                    paramIndex++;
                                    if (diff == null)
                                    {
                                        //This is for stand-alone tables. There is only one dimension table
                                        var paramDIdx = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = $"@__didx{didxParam}", Value = dimension.DIdx };
                                        parameters.Add(paramDIdx);
                                        var paramDVIdx = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = $"@__dvidx{dvidxParam}", Value = nextDVIdx };
                                        parameters.Add(paramDVIdx);
                                        didxParam++;
                                        dvidxParam++;

                                        sb.AppendLine($"if not exists(select * from [{dimensionValueTableName}] where [DIdx] = {paramDIdx.ParameterName} and [DVIdx] = {paramDVIdx.ParameterName})");
                                        sb.AppendLine($"insert into [{dimensionValueTableName}] ([DIdx], [DVIdx], [Value]) values ({paramDIdx.ParameterName}, {paramDVIdx.ParameterName}, {newParam.ParameterName})");
                                    }
                                    else
                                    {
                                        //This is for inherited tables. Figure out which dimension table to use
                                        var tempTable = dimensionValueTableNameParent;
                                        if (diff.DimensionList.Any(x => x.DIdx == dimension.DIdx))
                                            tempTable = dimensionValueTableName;

                                        var paramDIdx = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = $"@__didx{didxParam}", Value = dimension.DIdx };
                                        parameters.Add(paramDIdx);
                                        var paramDVIdx = new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = $"@__dvidx{dvidxParam}", Value = nextDVIdx };
                                        parameters.Add(paramDVIdx);
                                        didxParam++;
                                        dvidxParam++;
                                        
                                        sb.AppendLine($"if not exists(select * from [{tempTable}] where [DIdx] = {paramDIdx.ParameterName} and [DVIdx] = {paramDVIdx.ParameterName})");
                                        sb.AppendLine($"insert into [{tempTable}] ([DIdx], [DVIdx], [Value]) values ({paramDIdx.ParameterName}, {paramDVIdx.ParameterName}, {newParam.ParameterName})");
                                    }

                                    needSave = true;
                                }
                            }
                        }

                        if (needSave)
                        {
                            SqlHelper.ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), parameters, false);
                            Clear(id);
                            SqlHelper.MarkDimensionsChanged(id);
                            retval = GetCache(context, id, schema);
                            needSave = false;
                        }

                    }
                    #endregion

                    return retval.Results;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private class CacheResults
        {
            public CacheResults()
            {
                this.Timestamp = DateTime.Now;
            }

            public int RepositoryId { get; set; }
            public int? ParentId { get; set; }
            public List<DimensionItem> Results { get; set; }
            public DateTime Timestamp { get; set; }
            public int DimensionStamp { get; set; }

            ~CacheResults()
            {
                this.Results.Clear();
            }
        }

    }
}