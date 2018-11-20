using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.QueryBuilders
{
    internal class ListDimensionBuilder : IQueryBuilder
    {
        private ObjectConfiguration _configuration = null;
        private DimensionItem _newDimension = null;
        private DataSet _dsList = null;
        private string _sql = null;
        private List<SqlParameter> _listParameters = null;
        private Dictionary<long, IRefinementItem> _lookupRefinement = null;

        public ListDimensionBuilder(ObjectConfiguration configuration, DimensionItem newDimension)
        {
            this._configuration = configuration;
            this._newDimension = newDimension;

            //Try to get the Count objects from the cache
            //If not found it will be calculated below
            _lookupRefinement = ListDimensionCache.Get(_configuration.repositoryId, _newDimension.DIdx, _configuration.query);
        }

        private bool IsCachehit { get { return _lookupRefinement != null; } }

        public Task GenerateSql()
        {
            return Task.Factory.StartNew(() =>
            {
                //Nothing to do
                if (!_configuration.query.IncludeDimensions && !_configuration.query.IncludeRecords)
                    return;

                _newDimension.RefinementList.AddRange(_configuration.dimensionList.Where(x => x.DIdx == _newDimension.DIdx).SelectMany(x => x.RefinementList).ToClone<RefinementItem>().ToList());

                var listTable = SqlHelper.GetListTableName(_configuration.schema.ID, _newDimension.DIdx);

                //Each loop must determine if this dimension field is in base/inherited table
                var dimensionValueTableLoop = _configuration.dimensionValueTable;
                var dimensionTableLoop = _configuration.dimensionTable;

                if (_configuration.parentSchema != null && _configuration.parentSchema.DimensionList.Any(x => x.DIdx == _newDimension.DIdx))
                {
                    listTable = SqlHelper.GetListTableName(_configuration.schema.ParentID.Value, _newDimension.DIdx);
                    dimensionValueTableLoop = _configuration.dimensionValueTableParent;
                    dimensionTableLoop = _configuration.dimensionTableParent;
                }

                _listParameters = _configuration.parameters.ToList();
                var listParamValue = new SqlParameter() { ParameterName = "@__dvalue1", Value = _newDimension.DIdx };
                _listParameters.Add(listParamValue);

                #region Build Query
                //Sql2014 supports OFFSET/FETCH
                var sb = new StringBuilder();
                if (ConfigHelper.SupportsRowsFetch)
                {
                    var subfieldSql = _configuration.nonListDimensionDefs.Select(x => $"[__d{x.TokenName}]").Union(_configuration.normalFields.Select(x => $"[{x.TokenName}]")).ToCommaList();
                    if (!_configuration.query.IncludeRecords)
                    {
                        sb.AppendLine($"--MARKER 3" + _configuration.QueryPlanDebug);
                        sb.AppendLine($"SELECT CAST(0 AS BIGINT) AS {SqlHelper.RecordIdxField}, CAST(0 AS BIGINT) AS DVIdx, CAST('' AS NVARCHAR(500)) AS Value;");
                    }
                    else
                    {
                        var extraFields = string.Empty;
                        if (_configuration.orderByColumns.Any())
                            extraFields = ", " + _configuration.orderByFields.Select(x => x.GetSqlDefinition()).ToCommaList();

                        //Big records so do NOT select into temp table
                        sb.AppendLine($"--MARKER 2" + _configuration.QueryPlanDebug);
                        sb.Append($"WITH T ([{SqlHelper.RecordIdxField}]");
                        if (_configuration.orderByColumns.Any())
                            sb.Append("," + _configuration.orderByColumns.ToCommaList());
                        sb.Append(") AS (");
                        sb.Append($"    select " + (_configuration.hasFilteredListDims ? "DISTINCT" : string.Empty) + $" [Z].[{SqlHelper.RecordIdxField}]");
                        if (_configuration.orderByColumns.Any())
                            sb.Append("," + _configuration.orderByColumns.Select(x => "Z." + x).ToCommaList());
                        sb.AppendLine();
                        sb.AppendLine($"    FROM [{_configuration.dataTable}] Z {SqlHelper.NoLockText()}");
                        sb.AppendLine(_configuration.innerJoinClause);
                        sb.AppendLine($"    WHERE {_configuration.whereClause}");
                        sb.AppendLine($"    ORDER BY {_configuration.orderByClause}");
                        sb.AppendLine("    OFFSET (@startindex-1) ROWS FETCH FIRST (@endindex-@startindex) ROWS ONLY");
                        sb.AppendLine($"), S ([{SqlHelper.RecordIdxField}]) AS ( select distinct T.[{SqlHelper.RecordIdxField}] from T )");
                        sb.AppendLine($"SELECT Z.{SqlHelper.RecordIdxField}, DV.DVIdx, DV.Value");
                        sb.AppendLine($"FROM S Z " + SqlHelper.NoLockText());
                        sb.AppendLine($"inner join [{listTable}] Y {SqlHelper.NoLockText()} ON Y.{SqlHelper.RecordIdxField} = Z.{SqlHelper.RecordIdxField}");
                        sb.AppendLine($"inner join [{dimensionValueTableLoop}] DV {SqlHelper.NoLockText()} ON DV.DVIdx = Y.DVIdx AND DV.DIdx = {listParamValue.ParameterName}");
                        sb.AppendLine($"inner join [{dimensionTableLoop}] D {SqlHelper.NoLockText()} ON DV.DIdx = D.DIdx");
                        sb.AppendLine($"WHERE D.DIdx = {listParamValue.ParameterName}");
                        sb.AppendLine(";");
                    }
                    sb.AppendLine();

                    if (!this.IsCachehit) //only need the second query for counts
                    {
                        if (string.IsNullOrEmpty(_configuration.whereClause) || _configuration.whereClause == SqlHelper.EmptyWhereClause)
                        {
                            //If there is no WHERE clause then we can skip the whole Z table and 10x performance
                            sb.AppendLine($"--MARKER 45" + _configuration.QueryPlanDebug);
                            sb.AppendLine("SELECT DV.DVIdx, COUNT(DV.DVIdx)");
                            sb.AppendLine($"FROM [{listTable}] Y {SqlHelper.NoLockText()}");
                            sb.AppendLine($"inner join [{dimensionValueTableLoop}] DV {SqlHelper.NoLockText()} ON DV.DVIdx = Y.DVIdx AND DV.DIdx = {listParamValue.ParameterName}");
                            sb.AppendLine($"inner join [{dimensionTableLoop}] D {SqlHelper.NoLockText()} ON DV.DIdx = D.DIdx");
                            sb.AppendLine($"WHERE D.DIdx = {listParamValue.ParameterName}");
                            sb.AppendLine($"GROUP BY DV.DVIdx");
                        }
                        else
                        {
                            //Default way of calling list table
                            sb.AppendLine($"--MARKER 9" + _configuration.QueryPlanDebug);
                            sb.AppendLine($"WITH S ([{SqlHelper.RecordIdxField}])");
                            sb.AppendLine("AS");
                            sb.AppendLine("(");
                            sb.AppendLine("    select " + (_configuration.hasFilteredListDims ? "DISTINCT" : string.Empty) + $" [Z].[{SqlHelper.RecordIdxField}] FROM [{_configuration.dataTable}] Z {SqlHelper.NoLockText()}");
                            sb.AppendLine(_configuration.innerJoinClause);
                            sb.AppendLine($"    WHERE {_configuration.whereClause}");
                            sb.AppendLine(")");
                            sb.AppendLine("SELECT DV.DVIdx, COUNT(DV.DVIdx)");
                            sb.AppendLine($"FROM [{listTable}] Y {SqlHelper.NoLockText()}");
                            sb.AppendLine($"inner join [{dimensionValueTableLoop}] DV {SqlHelper.NoLockText()} ON DV.DVIdx = Y.DVIdx AND DV.DIdx = {listParamValue.ParameterName}");
                            sb.AppendLine($"inner join [{dimensionTableLoop}] D {SqlHelper.NoLockText()} ON DV.DIdx = D.DIdx");
                            sb.AppendLine($"inner join S Z {SqlHelper.NoLockText()} ON Y.{SqlHelper.RecordIdxField} = Z.{SqlHelper.RecordIdxField}");
                            sb.AppendLine($"WHERE D.DIdx = {listParamValue.ParameterName}");
                            sb.AppendLine("GROUP BY DV.DVIdx");
                        }
                    }

                }
                else
                {
                    if (!_configuration.query.IncludeRecords)
                    {
                        sb.AppendLine($"--MARKER 48");
                        sb.AppendLine($"SELECT CAST(0 AS BIGINT) AS {SqlHelper.RecordIdxField}, CAST(0 AS BIGINT) AS DVIdx, CAST('' AS NVARCHAR(500)) AS Value;");
                    }
                    else
                    {
                        sb.AppendLine($"--MARKER 11" + _configuration.QueryPlanDebug);
                        sb.AppendLine($"SELECT Z.{SqlHelper.RecordIdxField}, DV.DVIdx, DV.Value, [__RowNum] FROM (");
                        sb.AppendLine($"    SELECT [Z].[{SqlHelper.RecordIdxField}], [__RowNum] FROM (");
                        sb.AppendLine($"    SELECT ROW_NUMBER() OVER ( ORDER BY {_configuration.orderByClause} ) AS [__RowNum], [Z].[{SqlHelper.RecordIdxField}]");
                        sb.AppendLine($"    FROM [{_configuration.dataTable}] Z {SqlHelper.NoLockText()}");
                        sb.AppendLine(_configuration.innerJoinClause);
                        sb.AppendLine($"    WHERE {_configuration.whereClause}");
                        sb.AppendLine(") as Z ");
                        sb.AppendLine("WHERE ([Z].[__RowNum] >= @startindex AND [Z].[__RowNum] < @endindex)");
                        sb.AppendLine(") as Z");
                        sb.AppendLine($"inner join [{listTable}] Y {SqlHelper.NoLockText()} ON Y.{SqlHelper.RecordIdxField} = Z.{SqlHelper.RecordIdxField}");
                        sb.AppendLine($"inner join [{dimensionValueTableLoop}] DV {SqlHelper.NoLockText()} ON DV.DVIdx = Y.DVIdx AND DV.DIdx = {listParamValue.ParameterName}");
                        sb.AppendLine($"inner join [{dimensionTableLoop}] D {SqlHelper.NoLockText()} ON DV.DIdx = D.DIdx");
                        sb.AppendLine($"WHERE D.DIdx = {listParamValue.ParameterName}");
                        sb.AppendLine("ORDER BY [__RowNum];");
                    }
                    sb.AppendLine();

                    if (!this.IsCachehit) //only need the second query for counts
                    {
                        //TODO: This must be optimized with paging!!!!!! Really bad on 2008 machines with large # of refinements
                        sb.AppendLine($"--MARKER 12" + _configuration.QueryPlanDebug);
                        sb.AppendLine("SELECT DV.DVIdx, COUNT(DV.DVIdx)");
                        sb.AppendLine($"FROM [{_configuration.dataTable}] Z {SqlHelper.NoLockText()}");
                        sb.AppendLine(_configuration.innerJoinClause);
                        sb.AppendLine($"inner join [{listTable}] Y {SqlHelper.NoLockText()} ON Y.{SqlHelper.RecordIdxField} = Z.{SqlHelper.RecordIdxField}");
                        sb.AppendLine($"inner join [{dimensionValueTableLoop}] DV {SqlHelper.NoLockText()} ON DV.DVIdx = Y.DVIdx AND DV.DIdx = {listParamValue.ParameterName}");
                        sb.AppendLine($"inner join [{dimensionTableLoop}] D {SqlHelper.NoLockText()} ON DV.DIdx = D.DIdx");
                        sb.AppendLine($"WHERE {_configuration.whereClause}");
                        sb.AppendLine($"    AND D.DIdx = {listParamValue.ParameterName}");
                        sb.AppendLine("GROUP BY DV.DVIdx");
                    }
                }
                #endregion

                _sql = sb.ToString();
            });
        }

        public Task Execute()
        {
            return Task.Factory.StartNew(() =>
            {
                //Nothing to do
                if (!_configuration.query.IncludeDimensions && !_configuration.query.IncludeRecords)
                   return;

               var timer = Stopwatch.StartNew();
               try
               {
                   _dsList = SqlHelper.GetDataset(ConfigHelper.ConnectionString, _sql, _listParameters);
               }
               catch (Exception ex)
               {
                   RepositoryHealthMonitor.HealthCheck(_configuration.schema.ID);
                   DataManager.AddSkipItem(_configuration.schema.ID);
                   var message = ex.Message;
                   if (message.Contains("Timeout Expired"))
                       message = "Timeout Expired"; //Do not show whole message, no value
                    LoggerCQ.LogError($"ListDimensionBuilder: ID={_configuration.schema.ID}, DIdx={_newDimension?.DIdx}, Elapsed={timer.ElapsedMilliseconds}, Query=\"{_configuration.query.ToString()}\", Error={message}");
               }
           });
        }

        public Task Load()
        {
            return Task.Factory.StartNew(() =>
            {
                //Nothing to do
                if (!_configuration.query.IncludeDimensions && !_configuration.query.IncludeRecords)
                    return;
                if (_dsList == null)
                    return;

                if (_dsList.Tables.Count > 0)
                {
                    var recordCache = new Dictionary<long, List<string>>();
                    if (_configuration.query.IncludeRecords)
                    {
                        foreach (DataRow dr in _dsList.Tables[0].Rows)
                        {
                            var recordIdx = (long)dr[0];
                            var dvidx = (long)dr[1];
                            var value = (string)dr[2];
                            if (!recordCache.ContainsKey(recordIdx))
                                recordCache.Add(recordIdx, new List<string>());
                            recordCache[recordIdx].Add(value);
                        }
                    }

                    if (!this.IsCachehit)
                    {
                        //Put these in a lookup dictionary as there maybe thousands or more
                        //It makes lookup below much faster for these large sets
                        _lookupRefinement = _newDimension.RefinementList.ToDictionary(x => x.DVIdx, z => z);

                        foreach (DataRow dr in _dsList.Tables[1].Rows)
                        {
                            var dvidx = (long)dr[0];
                            var count = (int)dr[1];
                            if (_lookupRefinement.ContainsKey(dvidx))
                                _lookupRefinement[dvidx].Count = count;
                            else
                                LoggerCQ.LogWarning($"Cannot find DVIdx={dvidx}");
                        }
                        ListDimensionCache.Add(_configuration.repositoryId, _newDimension.DIdx, _configuration.query, _lookupRefinement);
                    }

                    //Only look in here if there are items
                    if (_lookupRefinement.Keys.Count != 0)
                    {
                        //This lambda has been replaced. There was some instanec where the 
                        //dictionary did NOT have the DVIdx value in it so it blew up
                        //Try to catch this case and log it
                        //_newDimension.RefinementList.ForEach(x => x.Count = _lookupRefinement[x.DVIdx].Count);
                        foreach (var ritem in _newDimension.RefinementList)
                        {
                            if (_lookupRefinement.ContainsKey(ritem.DVIdx))
                                ritem.Count = _lookupRefinement[ritem.DVIdx].Count;
                            else
                                LoggerCQ.LogWarning($"Missing Dictionary Value: ID={_configuration.schema.ID}, DIdx={ritem.DIdx}, DVIdx={ritem.DVIdx}, Value={ritem.FieldValue}");
                        }
                    }

                    //Now setup values for records
                    if (_configuration.query.IncludeRecords)
                    {
                        var fieldIndex = _configuration.schema.FieldList.Select(x => x.Name).ToList().IndexOf(_newDimension.Name);
                        //NOTE: there is a lock around "retval" as this is the only external object that is modified
                        lock (_configuration.retval)
                        {
                            foreach (var record in _configuration.retval.RecordList)
                            {
                                if (recordCache.ContainsKey(record.__RecordIndex))
                                    record.ItemArray[fieldIndex] = recordCache[record.__RecordIndex].ToArray();
                                else
                                    record.ItemArray[fieldIndex] = new string[] { };
                            }
                        }
                    }

                    if (!_configuration.query.IncludeEmptyDimensions)
                        _newDimension.RefinementList.RemoveAll(x => x.Count == 0);
                    _newDimension.RefinementList.RemoveAll(x => _configuration.query.DimensionValueList.Contains(x.DVIdx));

                    if (_configuration.query.IncludeDimensions)
                    {
                        lock (_configuration.retval)
                        {
                            _configuration.retval.DimensionList.Add(_newDimension);
                        }
                    }

                    lock (_configuration)
                    {
                        _configuration.PerfLoadLDim++;
                    }

                }
            });
        }

        public override string ToString()
        {
            return _newDimension?.Name;
        }
    }
}