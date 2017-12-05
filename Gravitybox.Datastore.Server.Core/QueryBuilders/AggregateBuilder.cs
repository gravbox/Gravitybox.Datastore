using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.QueryBuilders
{
    internal class AggregateBuilder : IQueryBuilder
    {
        private ObjectConfiguration _configuration = null;
        private DataSet _datset = null;
        private string _sql = null;

        public AggregateBuilder(ObjectConfiguration configuration)
        {
            _configuration = configuration;
        }

        public Task GenerateSql()
        {
            return Task.Factory.StartNew(() =>
            {
                //Do Nothing
                if (_configuration.query.DerivedFieldList == null)
                    return;

                var aggList = _configuration.query.DerivedFieldList
                    .Where(x => _configuration.schema.FieldList.Select(z => z.Name).Contains(x.Field))
                    .ToList();

                if (aggList.Count > 0)
                {
                    var sb = new StringBuilder();
                    sb.Append("SELECT ");
                    foreach (var field in aggList)
                    {
                        switch (field.Action)
                        {
                            case AggregateOperationConstants.Count:
                                sb.Append("COUNT([Z].[" + field.Field + "]), ");
                                break;
                            case AggregateOperationConstants.Max:
                                sb.Append("MAX([Z].[" + field.Field + "]), ");
                                break;
                            case AggregateOperationConstants.Min:
                                sb.Append("MIN([Z].[" + field.Field + "]), ");
                                break;
                            case AggregateOperationConstants.Sum:
                                sb.Append("SUM([Z].[" + field.Field + "]), ");
                                break;
                            case AggregateOperationConstants.Distinct:
                                sb.Append("COUNT(DISTINCT [Z].[" + field.Field + "]), ");
                                break;
                        }
                    }
                    sb.AppendLine("0");
                    sb.AppendLine("FROM [" + _configuration.dataTable + "] Z " + SqlHelper.NoLockText() + _configuration.innerJoinClause);
                    sb.AppendLine("WHERE " + _configuration.whereClause);
                    _sql = sb.ToString();
                }
            });
        }

        public Task Execute()
        {
            return Task.Factory.StartNew(() =>
            {
                //Do Nothing
                if (_configuration.query.DerivedFieldList == null || string.IsNullOrEmpty(_sql))
                    return;

                try
                {
                    _datset = SqlHelper.GetDataset(ConfigHelper.ConnectionString, _sql, _configuration.parameters);
                }
                catch (Exception ex)
                {
                    LoggerCQ.LogError(ex);
                }
            });
        }

        public Task Load()
        {
            return Task.Factory.StartNew(() =>
            {
                //Do Nothing
                if (_configuration.query.DerivedFieldList == null || string.IsNullOrEmpty(_sql))
                    return;

                var aggList = _configuration.query.DerivedFieldList.Where(x => _configuration.schema.FieldList.Select(z => z.Name).Contains(x.Field)).ToList();
                if (aggList.Count > 0)
                {
                    if (_datset.Tables.Count == 1 && _datset.Tables[0].Rows.Count == 1)
                    {
                        var returnAggs = new List<DerivedFieldValue>();
                        for (var ii = 0; ii < aggList.Count; ii++)
                        {
                            var orig = aggList[ii];
                            var newAgg = new DerivedFieldValue() { Action = orig.Action, Field = orig.Field, Alias = orig.Alias, Value = null };
                            if (orig.Action == AggregateOperationConstants.Count || orig.Action == AggregateOperationConstants.Distinct)
                            {
                                newAgg.Value = (int)_datset.Tables[0].Rows[0][ii];
                            }
                            else
                            {
                                if (_datset.Tables[0].Rows[0][ii] is int)
                                    newAgg.Value = (int)_datset.Tables[0].Rows[0][ii];
                                else if (_datset.Tables[0].Rows[0][ii] is long)
                                    newAgg.Value = (long)_datset.Tables[0].Rows[0][ii];
                                else if (_datset.Tables[0].Rows[0][ii] is string)
                                    newAgg.Value = (string)_datset.Tables[0].Rows[0][ii];
                                else if (_datset.Tables[0].Rows[0][ii] is DateTime)
                                    newAgg.Value = (DateTime)_datset.Tables[0].Rows[0][ii];
                                else if (_datset.Tables[0].Rows[0][ii] is bool)
                                    newAgg.Value = (bool)_datset.Tables[0].Rows[0][ii];
                                else if (_datset.Tables[0].Rows[0][ii] == System.DBNull.Value)
                                {
                                    switch (newAgg.Action)
                                    {
                                        case AggregateOperationConstants.Count: newAgg.Value = 0; break;
                                        case AggregateOperationConstants.Sum: newAgg.Value = 0; break;
                                    }
                                }
                                else
                                    throw new Exception("Unknown aggregate type returned");
                            }

                            returnAggs.Add(newAgg);
                        }

                        lock (_configuration.retval)
                        {
                            _configuration.retval.DerivedFieldList = returnAggs.ToArray();
                        }
                        _configuration.PerfLoadAgg = true;
                    }
                }
            });
        }

    }
}