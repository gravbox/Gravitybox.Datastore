using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Data;
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
            //Console.WriteLine("NormalDimensionBuilder:GenerateSql:Start");
            return Task.Factory.StartNew(() =>
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
                    var subList = tempDimList.Take(GBSize).Select(x => "[Z].[__d" + x.TokenName + "]").ToList();
                    while (subList.Count > 0)
                    {
                        if (_configuration.hasFilteredListDims)
                        {
                            var subListNaked = tempDimList.Take(GBSize).Select(x => $"[__d{x.TokenName}]").ToList();
                            var subListTVar = tempDimList.Take(GBSize).Select(x => $"T.[__d{x.TokenName}]").ToList();
                            sbSql.AppendLine("WITH T ([" + SqlHelper.RecordIdxField + "]," + string.Join(",", subListNaked) + " ) AS (");
                            sbSql.AppendLine("SELECT DISTINCT [Z].[" + SqlHelper.RecordIdxField + "]," + string.Join(",", subList));
                            sbSql.AppendLine("FROM [" + _configuration.dataTable + "] Z " + SqlHelper.NoLockText() + _configuration.innerJoinClause + " ");
                            sbSql.AppendLine("WHERE " + _configuration.whereClause + ")");
                            sbSql.AppendLine("SELECT count(*)," + string.Join(",", subListTVar));
                            sbSql.AppendLine("FROM T " + SqlHelper.NoLockText());
                            sbSql.AppendLine("group by grouping sets (");
                            sbSql.AppendLine(string.Join(",", subListTVar));
                            sbSql.AppendLine(");");
                        }
                        else
                        {
                            sbSql.Append("SELECT count(*)");
                            var realFieldList = tempDimList.Take(GBSize).ToList();
                            foreach (var item in realFieldList)
                            {
                                var fieldName = "[Z].[__d" + item.TokenName + "]";
                                if (_configuration.schema.FieldList.Any(x => x == item)) sbSql.Append(", " + fieldName);
                                else sbSql.Append(", NULL AS " + fieldName.Replace("[Z].", string.Empty));
                            }
                            sbSql.AppendLine();

                            sbSql.AppendLine("FROM [" + _configuration.dataTable + "] Z " + SqlHelper.NoLockText() + _configuration.innerJoinClause + " ");
                            sbSql.AppendLine("WHERE " + _configuration.whereClause + " ");
                            sbSql.AppendLine("group by grouping sets (");
                            sbSql.Append((firstGroup ? "()," : string.Empty)); //The "()" is the COUNT field = no grouping

                            var index = 0;
                            foreach (var item in realFieldList)
                            {
                                var fieldName = "[Z].[__d" + item.TokenName + "]";
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
                        subList = tempDimList.Take(GBSize).Select(x => "[Z].[__d" + x.TokenName + "]").ToList();
                        _configuration.dimensionGroups++;
                        firstGroup = false;
                        _doExecute = true;
                    }
                }
                _sql = sbSql.ToString().Replace(" AND "+ SqlHelper.EmptyWhereClause, string.Empty).Replace(" WHERE "+ SqlHelper.EmptyWhereClause, string.Empty);
                //Console.WriteLine("NormalDimensionBuilder:GenerateSql:Complete");
            });
        }

        public Task Execute()
        {
            //Console.WriteLine("NormalDimensionBuilder:Execute:Start");
            return Task.Factory.StartNew(() =>
            {
                //Nothing to do
                if (!_configuration.query.IncludeDimensions)
                    return;

                if (_doExecute)
                {
                    try
                    {
                        _datset = SqlHelper.GetDataset(ConfigHelper.ConnectionString, _sql, _configuration.parameters);
                    }
                    catch (Exception ex)
                    {
                        RepositoryHealthMonitor.HealthCheck(_configuration.schema.ID);
                        DataManager.AddSkipItem(_configuration.schema.ID);
                        LoggerCQ.LogError("NormalDimensionBuilder: ID=" + _configuration.schema.ID + ", Error=" + ex.Message);
                    }
                }
                //Console.WriteLine("NormalDimensionBuilder:Execute:Complete");
            });
        }

        public Task Load()
        {
            //Console.WriteLine("NormalDimensionBuilder:Load:Start");
            return Task.Factory.StartNew(() =>
            {
                //Nothing to do
                if (!_configuration.query.IncludeDimensions)
                    return;
                if (_datset == null) return;

                if (_configuration.query.IncludeDimensions)
                {
                    lock (_configuration.retval)
                    {
                        _configuration.retval.DimensionList = _configuration.nonListDimensionDefs
                        .Select(dimension => new DimensionItem()
                        {
                            DIdx = dimension.DIdx,
                            Name = dimension.Name,
                            Sortable = true,
                            NumericBreak = dimension.NumericBreak,
                        }).ToList();
                    }

                    //Faster lookup. this way we do not need to select on every loop
                    var allRefinements = _configuration.dimensionList
                                        .SelectMany(x => x.RefinementList)
                                        .ToDictionary(x => x.DVIdx, x => x);

                    for (var ii = 0; ii < _configuration.dimensionGroups; ii++) //loop for each 32 item group
                    {
                        var tt = _datset.Tables[ii];
                        foreach (DataRow dr in tt.Rows)
                        {
                            var count = (int)dr[0];
                            var wasFound = false;
                            for (var jj = 1; jj < tt.Columns.Count; jj++)
                            {
                                if (dr[jj] != System.DBNull.Value)
                                {
                                    long dvidx = 0;
                                    if (dr[jj] != DBNull.Value) dvidx = (long)dr[jj];
                                    if (dvidx != 0)
                                    {
                                        if (allRefinements.ContainsKey(dvidx))
                                        {
                                            var v = allRefinements[dvidx];
                                            var newDimension = _configuration.retval.DimensionList[(ii * GBSize) + jj - 1];
                                            newDimension.RefinementList.Add(new RefinementItem
                                            {
                                                DVIdx = dvidx,
                                                FieldValue = v.FieldValue,
                                                Count = count,
                                                DIdx = newDimension.DIdx
                                            });
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
                //Console.WriteLine("NormalDimensionBuilder:Load:Complete");
            });
        }

    }
}