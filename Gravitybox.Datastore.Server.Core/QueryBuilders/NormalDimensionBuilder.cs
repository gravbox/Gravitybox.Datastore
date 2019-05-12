using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.QueryBuilders
{
    internal class NormalDimensionBuilder : IQueryBuilder
    {
        const int GBSize = 32;
        private ObjectConfiguration _configuration = null;
        private DataSet _datset = null;
        private string _sql = null;
        private bool _doExecute = false;

        public NormalDimensionBuilder(ObjectConfiguration configuration)
        {
            _configuration = configuration;
        }

        public Task GenerateSql()
        {
            return Task.Factory.StartNew(() =>
            {
                try
                {
                    //Nothing to do
                    if (_configuration == null)
                        return;
                    else if (!_configuration.query.IncludeDimensions)
                        return;

                    var sbSql = new StringBuilder();

                    //Use the 'GROUPING SETS' syntax for less hits and parallelism
                    _configuration.UseGroupingSets = false;
                    _configuration.dimensionGroups = 0;

                    if (_configuration.query.IncludeDimensions)
                    {
                        var firstGroup = true;
                        var tempDimList = _configuration.nonListDimensionDefs.ToList();

                        //Remove normal dimensions that are already in the Query "DimensionValueList" since these will not show up in the returned dimension list
                        //These would cause a useless query where any data returned is ignored anyway
                        if (_configuration.query.DimensionValueList?.Any() == true && !_configuration.query.IncludeAllDimensions && !_configuration.query.IncludeEmptyDimensions)
                        {
                            _configuration.query.DimensionValueList.ForEach(x => tempDimList.RemoveAll(z => z.DIdx == Extensions.GetDIdxFromDVIdx(x)));
                        }

                        var subList = tempDimList.Take(GBSize).Select(x => $"[Z].[__d{x.TokenName}]").ToList();
                        while (subList.Count > 0)
                        {
                            if (_configuration.hasFilteredListDims)
                            {
                                var subListNaked = tempDimList.Take(GBSize).Select(x => $"[__d{x.TokenName}]").ToList();
                                var subListTVar = tempDimList.Take(GBSize).Select(x => $"T.[__d{x.TokenName}]").ToList();
                                sbSql.AppendLine($"--MARKER 37" + _configuration.QueryPlanDebug);
                                sbSql.AppendLine($"WITH T ([{SqlHelper.RecordIdxField}],{subListNaked.ToCommaList()}) AS (");
                                sbSql.AppendLine($"SELECT DISTINCT [Z].[{SqlHelper.RecordIdxField}],{subList.ToCommaList()}");
                                sbSql.AppendLine($"FROM [{_configuration.dataTable}] Z {SqlHelper.NoLockText()}{_configuration.innerJoinClause} ");
                                sbSql.AppendLine($"WHERE {_configuration.whereClause})");
                                sbSql.AppendLine($"SELECT count(*),{subListTVar.ToCommaList()}");
                                sbSql.AppendLine($"FROM T {SqlHelper.NoLockText()}");
                                sbSql.AppendLine("group by grouping sets (");
                                sbSql.AppendLine(subListTVar.ToCommaList());
                                sbSql.AppendLine(");");
                            }
                            else
                            {
                                sbSql.AppendLine($"--MARKER 38" + _configuration.QueryPlanDebug);
                                sbSql.Append("SELECT count(*)");
                                var realFieldList = tempDimList.Take(GBSize).ToList();
                                foreach (var item in realFieldList)
                                {
                                    var fieldName = $"[Z].[__d{item.TokenName}]";
                                    if (_configuration.schema.FieldList.Any(x => x == item)) sbSql.Append(", " + fieldName);
                                    else sbSql.Append(", NULL AS " + fieldName.Replace("[Z].", string.Empty));
                                }
                                sbSql.AppendLine();

                                sbSql.AppendLine($"FROM [{_configuration.dataTable}] Z {SqlHelper.NoLockText()}{_configuration.innerJoinClause} ");
                                sbSql.AppendLine($"WHERE {_configuration.whereClause} ");
                                sbSql.AppendLine("group by grouping sets (");
                                sbSql.Append((firstGroup ? "()," : string.Empty)); //The "()" is the COUNT field = no grouping

                                var index = 0;
                                foreach (var item in realFieldList)
                                {
                                    var fieldName = $"[Z].[__d{item.TokenName}]";
                                    if (index > 0) sbSql.Append(",");
                                    if (_configuration.schema.FieldList.Any(x => x == item)) sbSql.Append(fieldName);
                                    else sbSql.Append("()");
                                    index++;
                                }

                                sbSql.AppendLine(")");
                                _configuration.UseGroupingSets = true;
                            }
                            sbSql.AppendLine();
                            tempDimList = tempDimList.Skip(GBSize).ToList();
                            subList = tempDimList.Take(GBSize).Select(x => $"[Z].[__d{x.TokenName}]").ToList();
                            _configuration.dimensionGroups++;
                            firstGroup = false;
                            _doExecute = true;
                        }
                    }
                    _sql = sbSql.ToString()
                        .Replace($" AND {SqlHelper.EmptyWhereClause}", string.Empty)
                        .Replace($" WHERE {SqlHelper.EmptyWhereClause}", string.Empty);

                }
                catch (Exception ex)
                {
                    LoggerCQ.LogError(ex, $"NormalDimensionBuilder: ID={_configuration.schema.ID}, Query=\"{_configuration.query.ToString()}\", Error={ex.Message}");
                }
            });
        }

        public Task Execute()
        {
            return Task.Factory.StartNew(() =>
            {
                //Nothing to do
                if (!_configuration.query.IncludeDimensions)
                    return;

                if (_doExecute)
                {
                    var timer = Stopwatch.StartNew();
                    try
                    {
                        _datset = SqlHelper.GetDataset(ConfigHelper.ConnectionString, _sql, _configuration.parameters);

                        //Log long running
                        timer.Stop();
                        if (timer.ElapsedMilliseconds > 10000)
                            LoggerCQ.LogWarning($"NormalDimensionBuilderDelay: ID={_configuration.schema.ID}, Elapsed={timer.ElapsedMilliseconds}, Query=\"{_configuration.query.ToString()}\"");
                    }
                    catch (Exception ex)
                    {
                        RepositoryHealthMonitor.HealthCheck(_configuration.schema.ID);
                        DataManager.AddSkipItem(_configuration.schema.ID);
                        LoggerCQ.LogError(ex, $"NormalDimensionBuilder: ID={_configuration.schema.ID}, Query=\"{_configuration.query.ToString()}\", Error={ex.Message}");
                    }
                }
            });
        }

        public Task Load()
        {
            return Task.Factory.StartNew(() =>
            {
                try
                {
                    //Nothing to do
                    if (!_configuration.query.IncludeDimensions)
                        return;
                    if (_datset == null) return;

                    if (_configuration.query.IncludeDimensions)
                    {
                        lock (_configuration.retval)
                        {
                            _configuration.retval.DimensionList.AddRange(_configuration.nonListDimensionDefs
                                .Select(dimension => new DimensionItem()
                                {
                                    DIdx = dimension.DIdx,
                                    Name = dimension.Name,
                                    Sortable = true,
                                    NumericBreak = dimension.NumericBreak,
                                }).ToList());
                        }

                        //Faster lookup. this way we do not need to select on every loop
                        var allRefinements = _configuration.dimensionList
                                            .SelectMany(x => x.RefinementList)
                                            .ToDictionary(x => x.DVIdx, x => x);

                        //hash dimensions for faster access
                        Dictionary<long, DimensionItem> dimensionHash = null;
                        lock (_configuration.retval)
                        {
                            dimensionHash = _configuration.retval.DimensionList.ToDictionary(x => x.DIdx, x => x);
                        }

                        for (var ii = 0; ii < _configuration.dimensionGroups; ii++) //loop for each 32 item group
                        {
                            var tt = _datset.Tables[ii];
                            foreach (DataRow dr in tt.Rows)
                            {
                                var count = (int)dr[0];
                                var wasFound = false;
                                for (var jj = 1; jj < tt.Columns.Count; jj++)
                                {
                                    if (dr[jj] != DBNull.Value)
                                    {
                                        var dvidx = (long)dr[jj];
                                        if (dvidx != 0)
                                        {
                                            if (allRefinements.TryGetValue(dvidx, out IRefinementItem v))
                                            {
                                                var newDimension = dimensionHash[v.DIdx];
                                                var rItem = new RefinementItem
                                                {
                                                    DVIdx = dvidx,
                                                    FieldValue = v.FieldValue,
                                                    Count = count,
                                                    DIdx = newDimension.DIdx,
                                                };

                                                if (newDimension.NumericBreak != null)
                                                {
                                                    rItem.MinValue = rItem.FieldValue.ToInt64();
                                                    if (rItem.MinValue != null)
                                                        rItem.MaxValue = rItem.MinValue + newDimension.NumericBreak;
                                                }
                                                newDimension.RefinementList.Add(rItem);
                                                wasFound = true;
                                            }
                                        }
                                        break;
                                    }
                                }

                                //This is the non-group, COUNT value
                                if (!wasFound && _configuration.UseGroupingSets && _configuration.retval.TotalRecordCount < count)
                                {
                                    lock (_configuration.retval)
                                    {
                                        _configuration.retval.TotalRecordCount = count;
                                    }
                                }

                            }
                        }

                        _configuration.PerfLoadNDim = true;

                    } //IncludeDimensions
                }
                catch (Exception ex)
                {
                    LoggerCQ.LogError(ex, $"NormalDimensionBuilder: ID={_configuration.schema.ID}, Query=\"{_configuration.query.ToString()}\", Error={ex.Message}");
                }
            });
        }

    }
}