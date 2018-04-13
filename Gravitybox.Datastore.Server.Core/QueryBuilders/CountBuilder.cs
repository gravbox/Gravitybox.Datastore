using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.QueryBuilders
{
    internal class CountBuilder : IQueryBuilder
    {
        private ObjectConfiguration _configuration = null;
        private DataSet _datset = null;
        private string _sql = null;

        public CountBuilder(ObjectConfiguration configuration)
        {
            _configuration = configuration;
        }

        public Task GenerateSql()
        {
            //Console.WriteLine("CountBuilder:GenerateSql:Start");
            return Task.Factory.StartNew(() =>
            {
                //Nothing to do
                if (_configuration.query.ExcludeCount)
                    return;

                if (_configuration.IsGrouped)
                {
                    #region Grouping SQL
                    string groupSql = null;
                    {
                        var groupListSql = new List<string>();
                        var fields = _configuration.schema.FieldList;
                        if (_configuration.query.GroupFields?.Count > 0)
                        {
                            fields = fields.Where(x => _configuration.query.GroupFields.Contains(x.Name)).ToList();
                            if (fields.Count != _configuration.query.GroupFields.Count)
                                throw new Exception("Unknown GroupFields in explicit select.");
                        }

                        foreach (var field in fields)
                        {
                            groupListSql.Add($"[Z].[{field.TokenName}]");
                        }

                        groupSql = groupListSql.ToCommaList();
                    }
                    #endregion

                    //The query is a GroupBy so find count of groups NOT records
                    var sbSql = new StringBuilder();

                    //If we calculated the count in the GROUPING statement then skip this
                    sbSql.AppendLine($"--MARKER 20");
                    sbSql.AppendLine("SELECT COUNT(*) from (");
                    sbSql.AppendLine($"SELECT {groupSql}, COUNT(*) AS C");
                    sbSql.AppendLine($"FROM [{_configuration.dataTable}] Z {SqlHelper.NoLockText()} {_configuration.innerJoinClause}");
                    sbSql.AppendLine($"WHERE {_configuration.whereClause}");
                    sbSql.AppendLine($"GROUP BY {groupSql}");
                    sbSql.AppendLine(") AS K");

                    _sql = sbSql.ToString()
                                .Replace($" AND {SqlHelper.EmptyWhereClause}", string.Empty)
                                .Replace($" WHERE {SqlHelper.EmptyWhereClause}", string.Empty);
                }
                else if (!_configuration.UseGroupingSets)
                {
                    var sbSql = new StringBuilder();
                    if (!_configuration.query.ExcludeCount)
                    {
                        //If we calculated the count in the GROUPING statement then skip this
                        sbSql.AppendLine($"--MARKER 21");
                        sbSql.AppendLine($"SELECT COUNT(DISTINCT([Z].[{SqlHelper.RecordIdxField}]))");
                        sbSql.AppendLine($"FROM [{_configuration.dataTable}] Z {SqlHelper.NoLockText()} {_configuration.innerJoinClause}");
                        sbSql.AppendLine($"WHERE {_configuration.whereClause}");
                    }
                    else
                    {
                        sbSql.AppendLine("SELECT 0");
                    }
                    _sql = sbSql.ToString().Replace(" AND " + SqlHelper.EmptyWhereClause, string.Empty).Replace(" WHERE " + SqlHelper.EmptyWhereClause, string.Empty);
                }
                //Console.WriteLine("CountBuilder:GenerateSql:Complete");
            });
        }

        public Task Execute()
        {
            //Console.WriteLine("CountBuilder:Execute:Start");
            return Task.Factory.StartNew(() =>
            {
                //Nothing to do
                if (_configuration.query.ExcludeCount)
                    return;

                if (!_configuration.UseGroupingSets)
                {
                    try
                    {
                        _datset = SqlHelper.GetDataset(ConfigHelper.ConnectionString, _sql, _configuration.parameters);
                        //Console.WriteLine("CountBuilder:Execute:Complete");
                    }
                    catch (Exception ex)
                    {
                        LoggerCQ.LogError(ex);
                    }
                }
            });
        }

        public Task Load()
        {
            //Console.WriteLine("CountBuilder:Load:Start");
            return Task.Factory.StartNew(() =>
            {
                //Nothing to do
                if (_configuration.query.ExcludeCount)
                    return;

                if (!_configuration.UseGroupingSets)
                {
                    var v = (int)_datset.Tables[0].Rows[0][0];
                    lock (_configuration.retval)
                    {
                        _configuration.retval.TotalRecordCount = v;
                    }
                    _configuration.PerfLoadCount = true;
                }
                //Console.WriteLine("CountBuilder:Load:Complete");
            });
        }

    }
}
