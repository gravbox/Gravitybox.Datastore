using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.QueryBuilders
{
    internal class RecordBuilder : IQueryBuilder
    {
        private ObjectConfiguration _configuration = null;
        private DataSet _datset = null;
        private string _sql = null;
        private bool _isNormal = true;

        public RecordBuilder(ObjectConfiguration configuration)
        {
            _configuration = configuration;
        }

        public Task GenerateSql()
        {
            //Console.WriteLine("RecordBuilder:GenerateSql:Start");

            //If this is a grouping with specific select fields then use the group by
            if (_configuration.IsGrouped)
            {
                _isNormal = false;
                return Task.Factory.StartNew(() =>
                {
                    GenerateSqlGrouped();
                });
            }
            else
            {
                _isNormal = true;
                return Task.Factory.StartNew(() =>
                {
                    GenerateSqlNormal();
                });
            }
        }

        private void GenerateSqlGrouped()
        {
            #region Fields SQL
            string fieldSql = null;
            {
                var fieldListSql = new List<string>();
                var fields = _configuration.schema.FieldList;
                if (_configuration.IsGrouped)
                {
                    //fields = fields.Where(x => _configuration.query.DerivedFieldList.Select(z => z.Field).Contains(x.Name)).ToList();
                    //if (fields.Count != _configuration.query.DerivedFieldList.Count)
                    //    throw new Exception("Unknown fields in explicit select.");
                    _configuration.usingCustomSelect = ObjectConfiguration.SelectionMode.Grouping;
                }

                //Only supports 1 group by field for now
                foreach (var gItem in _configuration.query.GroupFields)
                {
                    fieldListSql.Add("[Z].[" + Utilities.DbTokenize(gItem) + "]");
                }

                foreach (DerivedField dField in _configuration.query.DerivedFieldList)
                {
                    var field = fields.FirstOrDefault(x => x.Name == dField.Field);
                    if (field?.DataType == RepositorySchema.DataTypeConstants.List)
                        fieldListSql.Add("0 AS [__" + field.TokenName + "]");
                    else
                    {
                        if (_configuration.query.GroupFields.Any(x => x == field?.Name))
                        {
                            fieldListSql.Add("[Z].[" + field.TokenName + "]");
                        }
                        else
                        {
                            switch (dField.Action)
                            {
                                case AggregateOperationConstants.Count:
                                    fieldListSql.Add("COUNT(*) AS [" + dField.TokenName + "]");
                                    break;
                                case AggregateOperationConstants.Max:
                                    fieldListSql.Add("MAX([Z].[" + field.TokenName + "]) AS [" + dField.TokenName + "]");
                                    break;
                                case AggregateOperationConstants.Min:
                                    fieldListSql.Add("MIN([Z].[" + field.TokenName + "]) AS [" + dField.TokenName + "]");
                                    break;
                                case AggregateOperationConstants.Sum:
                                    fieldListSql.Add("SUM([Z].[" + field.TokenName + "]) AS [" + dField.TokenName + "]");
                                    break;
                                //case AggregateOperationConstants.Distinct:
                                default:
                                    throw new Exception("Select fields must have an aggregation type!");
                            }
                        }
                    }
                }

                fieldSql = fieldListSql.ToCommaList();
            }
            #endregion

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

            //TODO: add some sort of ordering that is not hard-wired

            var sbSql = new StringBuilder();

            #region Records
            _configuration.isGeo = false;
            if (ConfigHelper.SupportsRowsFetch)
            {
                if (_configuration.query.RecordsPerPage <= 0 || _configuration.query.RecordsPerPage == int.MaxValue)
                {
                    //No paging...this is faster
                    sbSql.AppendLine($"SELECT {fieldSql}");
                    sbSql.AppendLine($"FROM [{_configuration.dataTable}] Z {SqlHelper.NoLockText()}{_configuration.innerJoinClause}");
                    sbSql.AppendLine($"WHERE {_configuration.whereClause}");
                    sbSql.AppendLine($"GROUP BY {groupSql}");
                    sbSql.AppendLine($"ORDER BY {groupSql}");
                }
                else
                {
                    //Big records so do NOT select into temp table
                    sbSql.AppendLine($"WITH T ([{SqlHelper.RecordIdxField}]) AS (");
                    sbSql.AppendLine($"SELECT {(_configuration.hasFilteredListDims ? "DISTINCT" : string.Empty)} [Z].[{SqlHelper.RecordIdxField}]");
                    sbSql.AppendLine($"FROM [{_configuration.dataTable}] Z {SqlHelper.NoLockText()}{_configuration.innerJoinClause}");
                    sbSql.AppendLine($"WHERE {_configuration.whereClause}");
                    sbSql.AppendLine($"), S ([{SqlHelper.RecordIdxField}]) AS ( select distinct T.[{SqlHelper.RecordIdxField}] from T )");
                    sbSql.AppendLine($"SELECT {fieldSql}");
                    sbSql.AppendLine($"FROM [{_configuration.dataTable}] Z {SqlHelper.NoLockText()} inner join S on [Z].[{SqlHelper.RecordIdxField}] = S.[{SqlHelper.RecordIdxField}]");
                    sbSql.AppendLine($"GROUP BY {groupSql}");
                    sbSql.AppendLine($"ORDER BY {groupSql}");
                    sbSql.AppendLine($"OFFSET (@startindex-1) ROWS FETCH FIRST (@endindex-@startindex) ROWS ONLY");
                }
                sbSql.AppendLine(";");
            }
            else
            {
                throw new Exception("Functionality requires SQL Server 2012 or newer.");
            }
            sbSql.AppendLine();

            var startIndex = ((_configuration.query.PageOffset - 1) * _configuration.query.RecordsPerPage) + 1;
            if (startIndex <= 0) startIndex = 1;
            var endIndex = (startIndex + _configuration.query.RecordsPerPage) + _configuration.extraRecords;
            if (endIndex < 0) endIndex = int.MaxValue;
            _configuration.parameters.Add(new SqlParameter { DbType = DbType.Int32, ParameterName = "@startindex", Value = startIndex });
            _configuration.parameters.Add(new SqlParameter { DbType = DbType.Int32, ParameterName = "@endindex", Value = endIndex });
            _configuration.parameters.Add(new SqlParameter { DbType = DbType.Int32, ParameterName = "@repositoryid", Value = _configuration.repositoryId });

            #endregion

            _sql = sbSql.ToString().Replace(" AND " + SqlHelper.EmptyWhereClause, string.Empty).Replace(" WHERE " + SqlHelper.EmptyWhereClause, string.Empty);
            //Console.WriteLine("RecordBuilder:GenerateSql:Complete");
        }

        private void GenerateSqlNormal()
        {
            if (_configuration == null) return;

            #region Fields SQL
            var fieldListSql = new List<string>();
            var fields = _configuration.schema.FieldList;
            if (_configuration.query.FieldSelects?.Count > 0)
            {
                fields = fields.Where(x => _configuration.query.FieldSelects.Contains(x.Name)).ToList();
                if (fields.Count != _configuration.query.FieldSelects.Count)
                    throw new Exception("Unknown fields in explicit select.");
                if (!fields.Contains(_configuration.schema.PrimaryKey))
                    fields.Add(_configuration.schema.PrimaryKey);
                _configuration.usingCustomSelect = ObjectConfiguration.SelectionMode.Custom;
            }

            foreach (var field in fields)
            {
                if (field.DataType == RepositorySchema.DataTypeConstants.List) fieldListSql.Add($"0 AS [__{field.TokenName}]");
                else fieldListSql.Add($"[Z].[{field.TokenName}]");
            }

            var fieldSql = $"{fieldListSql.ToCommaList()}, [Z].[{SqlHelper.RecordIdxField}], [Z].[{SqlHelper.TimestampField}], [Z].[{SqlHelper.HashField}]";

            #endregion

            var sbSql = new StringBuilder();

            #region Records
            _configuration.isGeo = false;
            //If supports OFFSET/FETCH then use it
            if (!_configuration.query.IncludeRecords)
            {
                sbSql.AppendLine("SELECT 1;");
            }
            else if (ConfigHelper.SupportsRowsFetch)
            {
                if (_configuration.query.RecordsPerPage <= 0 || _configuration.query.RecordsPerPage == int.MaxValue)
                {
                    //No paging...this is faster
                    sbSql.AppendLine($"SELECT {fieldSql}");
                    sbSql.AppendLine($"FROM [{_configuration.dataTable}] Z {SqlHelper.NoLockText()}{_configuration.innerJoinClause}");
                    sbSql.AppendLine($"WHERE {_configuration.whereClause}");
                    sbSql.AppendLine($"ORDER BY {_configuration.orderByClause}");
                }
                #region OLD CODE - There is no need to select into a temp object
                //else if (_configuration.query.RecordsPerPage <= SqlHelper.SmallRecordBlock && !_configuration.hasFilteredListDims)
                //{
                //    var geoFilter = _configuration.query.FieldFilters.FirstOrDefault(x => x.DataType == RepositorySchema.DataTypeConstants.GeoCode) as GeoCodeFieldFilter;
                //    _configuration.isGeo = (geoFilter != null);

                //    //Sub-select into a table variable is faster with a small amount of records
                //    sbSql.AppendLine("DECLARE @RecordIds TABLE ([" + SqlHelper.RecordIdxField + "] bigint PRIMARY KEY, [__SortOrder] INT NOT NULL identity(1,1)" + (_configuration.isGeo ? ", [__Distance] float" : string.Empty) + ")");
                //    sbSql.AppendLine("INSERT INTO @RecordIds ([" + SqlHelper.RecordIdxField + "]" + (_configuration.isGeo ? ", [__Distance]" : string.Empty) + ")");
                //    sbSql.AppendLine("SELECT [Z].[" + SqlHelper.RecordIdxField + "]" + (_configuration.isGeo ? ", [Z].[" + geoFilter.Name + "].STDistance(geography::Point(" + geoFilter.Latitude + ", " + geoFilter.Longitude + ", 4326)) / 1609.344 AS [__Distance]" : string.Empty));
                //    sbSql.AppendLine("FROM [" + _configuration.dataTable + "] Z " + SqlHelper.NoLockText() + _configuration.innerJoinClause);
                //    sbSql.AppendLine("WHERE " + _configuration.whereClause);
                //    sbSql.AppendLine("ORDER BY " + _configuration.orderByClause);
                //    sbSql.AppendLine("OFFSET (@startindex-1) ROWS FETCH FIRST (@endindex-@startindex) ROWS ONLY;");
                //    sbSql.AppendLine();
                //    sbSql.AppendLine("SELECT " + fieldSql + (_configuration.isGeo ? ", S.[__Distance]" : string.Empty));
                //    sbSql.AppendLine("FROM [" + _configuration.dataTable + "] Z " + SqlHelper.NoLockText() + " inner join @RecordIds S on [Z].[" + SqlHelper.RecordIdxField + "] = S.[" + SqlHelper.RecordIdxField + "]");
                //    sbSql.AppendLine("ORDER BY S.[__SortOrder]");
                //}
                #endregion
                else
                {
                    //Big records so do NOT select into temp table
                    sbSql.AppendLine("WITH T ([" + SqlHelper.RecordIdxField + "]) AS (");
                    sbSql.AppendLine("SELECT " + (_configuration.hasFilteredListDims ? "DISTINCT" : string.Empty) + " [Z].[" + SqlHelper.RecordIdxField + "]");
                    sbSql.AppendLine("FROM [" + _configuration.dataTable + "] Z " + SqlHelper.NoLockText() + _configuration.innerJoinClause);
                    sbSql.AppendLine("WHERE " + _configuration.whereClause);
                    sbSql.AppendLine("), S ([" + SqlHelper.RecordIdxField + "]) AS ( select distinct T.[" + SqlHelper.RecordIdxField + "] from T )");
                    sbSql.AppendLine("SELECT " + fieldSql);
                    sbSql.AppendLine("FROM [" + _configuration.dataTable + "] Z " + SqlHelper.NoLockText() + " inner join S on [Z].[" + SqlHelper.RecordIdxField + "] = S.[" + SqlHelper.RecordIdxField + "]");
                    sbSql.AppendLine("ORDER BY " + _configuration.orderByClause);
                    sbSql.AppendLine("OFFSET (@startindex-1) ROWS FETCH FIRST (@endindex-@startindex) ROWS ONLY");
                }
                sbSql.AppendLine(";");
            }
            else
            {
                sbSql.AppendLine("WITH Z AS (");
                sbSql.AppendLine("SELECT ROW_NUMBER() OVER ( ORDER BY " + _configuration.orderByClause + " ) AS [__RowNum], " + fieldSql + " FROM [" + _configuration.dataTable + "] Z " + SqlHelper.NoLockText() + _configuration.innerJoinClause + " WHERE " + _configuration.whereClause);
                sbSql.AppendLine(")");
                sbSql.AppendLine("SELECT " + fieldSql);
                sbSql.AppendLine("FROM Z");
                sbSql.AppendLine("where (@startindex) <= [__RowNum] AND [__RowNum] < (@endindex);");
            }
            sbSql.AppendLine();

            var startIndex = ((_configuration.query.PageOffset - 1) * _configuration.query.RecordsPerPage) + 1;
            if (startIndex <= 0) startIndex = 1;
            var endIndex = (startIndex + _configuration.query.RecordsPerPage) + _configuration.extraRecords;
            if (endIndex < 0) endIndex = int.MaxValue;
            _configuration.parameters.Add(new SqlParameter { DbType = DbType.Int32, ParameterName = "@startindex", Value = startIndex });
            _configuration.parameters.Add(new SqlParameter { DbType = DbType.Int32, ParameterName = "@endindex", Value = endIndex });
            _configuration.parameters.Add(new SqlParameter { DbType = DbType.Int32, ParameterName = "@repositoryid", Value = _configuration.repositoryId });

            #endregion

            _sql = sbSql.ToString()
                .Replace(" AND " + SqlHelper.EmptyWhereClause, string.Empty)
                .Replace(" WHERE " + SqlHelper.EmptyWhereClause, string.Empty);
            //Console.WriteLine("RecordBuilder:GenerateSql:Complete");
        }

        public Task Execute()
        {
            //Console.WriteLine("RecordBuilder:Execute:Start");
            return Task.Factory.StartNew(() =>
            {
                try
                {
                    _datset = SqlHelper.GetDataset(ConfigHelper.ConnectionString, _sql, _configuration.parameters);
                    //Console.WriteLine("RecordBuilder:Execute:Complete");
                }
                catch (Exception ex)
                {
                    RepositoryHealthMonitor.HealthCheck(_configuration.schema.ID);
                    DataManager.AddSkipItem(_configuration.schema.ID);
                    LoggerCQ.LogError(ex, $"RecordBuilder: ID={_configuration.schema.ID}, Error={ex.Message}");
                }
            });
        }

        public Task Load()
        {
            if (_isNormal)
            {
                return Task.Factory.StartNew(() =>
                {
                    LoadNormal();
                });
            }
            else
            {
                return Task.Factory.StartNew(() =>
                {
                    LoadGrouped();
                });
            }
        }

        private void LoadNormal()
        {
            _configuration.retval = new DataQueryResults();
            _configuration.retval.DimensionList = new List<DimensionItem>();
            _configuration.retval.AppliedDimensionList = new List<DimensionItem>();
            _configuration.retval.AllDimensionList = new List<DimensionItem>();
            _configuration.retval.Query = _configuration.query;
            _configuration.retval.QueryTime = DateTime.Now;
            _configuration.retval.VersionHash = RepositoryManager.GetSchemaHash(_configuration.schema.ID);

            _configuration.retval.RecordList = new List<DataItem>();
            if (_configuration.query.IncludeRecords)
            {
                var startOrdinal = (_configuration.query.PageOffset - 1) * _configuration.query.RecordsPerPage;
                if (_configuration.usingCustomSelect == ObjectConfiguration.SelectionMode.Custom)
                {
                    #region Load custom field set
                    var loadedColumns = _datset.Tables[0].Columns.ToList<DataColumn>();
                    foreach (DataRow dr in _datset.Tables[0].Rows)
                    {
                        var newItem = new DataItem();
                        newItem.__RecordIndex = (long)dr[SqlHelper.RecordIdxField];
                        newItem.__OrdinalPosition = startOrdinal++;
                        newItem.__Timestamp = (int)dr[SqlHelper.TimestampField];
                        newItem.__Hash = (long)dr[SqlHelper.HashField];
                        var itemArray = new List<object>();
                        foreach (var field in _configuration.schema.FieldList)
                        {
                            if (loadedColumns.Any(x => x.ColumnName == field.Name))
                            {
                                var columnName = _datset.Tables[0].Columns[field.Name].ColumnName;
                                if (field.DataType == RepositorySchema.DataTypeConstants.List)
                                    itemArray.Add(null); //List will be populated below
                                else if (dr[columnName] == DBNull.Value)
                                    itemArray.Add(null);
                                else if (dr[columnName] is Microsoft.SqlServer.Types.SqlGeography)
                                {
                                    var v = (Microsoft.SqlServer.Types.SqlGeography)dr[columnName];
                                    if (v.IsNull)
                                        itemArray.Add(null);
                                    else
                                    {
                                        var newGeo = new GeoCode() { Latitude = (double)v.Lat, Longitude = (double)v.Long };
                                        if (_configuration.isGeo) newGeo.Distance = (double)dr["__Distance"];
                                        itemArray.Add(newGeo);
                                    }
                                }
                                else
                                    itemArray.Add(dr[columnName]);
                            }
                            else
                                itemArray.Add(null); //Need proper count so all others are null
                        }
                        newItem.ItemArray = itemArray.ToArray();
                        _configuration.retval.RecordList.Add(newItem);
                    }
                    #endregion
                }
                else
                {
                    #region Load all fields
                    if (_datset == null) return;
                    foreach (DataRow dr in _datset.Tables[0].Rows)
                    {
                        var newItem = new DataItem();
                        newItem.__RecordIndex = (long)dr[SqlHelper.RecordIdxField];
                        newItem.__OrdinalPosition = startOrdinal++;
                        newItem.__Timestamp = (int)dr[SqlHelper.TimestampField];
                        newItem.__Hash = (long)dr[SqlHelper.HashField];
                        var itemArray = new List<object>();
                        var index = 0;
                        foreach (var field in _configuration.schema.FieldList)
                        {
                            if (field.DataType == RepositorySchema.DataTypeConstants.List)
                                itemArray.Add(null); //List will be populated below
                            else if (dr[index] == DBNull.Value)
                                itemArray.Add(null);
                            else if (dr[index] is Microsoft.SqlServer.Types.SqlGeography)
                            {
                                var v = (Microsoft.SqlServer.Types.SqlGeography)dr[index];
                                if (v.IsNull)
                                    itemArray.Add(null);
                                else
                                {
                                    var newGeo = new GeoCode() { Latitude = (double)v.Lat, Longitude = (double)v.Long };
                                    if (_configuration.isGeo) newGeo.Distance = (double)dr["__Distance"];
                                    itemArray.Add(newGeo);
                                }
                            }
                            else
                                itemArray.Add(dr[index]);
                            index++;
                        }
                        newItem.ItemArray = itemArray.ToArray();
                        _configuration.retval.RecordList.Add(newItem);
                    }
                    #endregion
                }
                _configuration.PerfLoadRecords = true;
            } //IncludeRecords
        }

        private void LoadGrouped()
        {
            _configuration.retval = new DataQueryResults();
            _configuration.retval.DimensionList = new List<DimensionItem>();
            _configuration.retval.AppliedDimensionList = new List<DimensionItem>();
            _configuration.retval.AllDimensionList = new List<DimensionItem>();
            _configuration.retval.Query = _configuration.query;
            _configuration.retval.QueryTime = DateTime.Now;
            _configuration.retval.VersionHash = RepositoryManager.GetSchemaHash(_configuration.schema.ID);

            _configuration.retval.RecordList = new List<DataItem>();

            var startOrdinal = (_configuration.query.PageOffset - 1) * _configuration.query.RecordsPerPage;
            #region Load custom field set
            var loadedColumns = _datset.Tables[0].Columns.ToList<DataColumn>();
            foreach (DataRow dr in _datset.Tables[0].Rows)
            {
                var newItem = new DataItem();
                newItem.__OrdinalPosition = startOrdinal++;
                var itemArray = new List<object>();

                //Load grouping fields
                foreach (var field in _configuration.query.GroupFields)
                {
                    if (loadedColumns.Any(x => x.ColumnName == field))
                    {
                        var columnName = _datset.Tables[0].Columns[field].ColumnName;
                        if (dr[columnName] == DBNull.Value)
                            itemArray.Add(null);
                        else
                            itemArray.Add(dr[columnName]);
                    }
                    else
                        itemArray.Add(null); //Need proper count so all others are null
                }

                //Load aggregate fields
                foreach (var field in _configuration.query.DerivedFieldList)
                {
                    if (loadedColumns.Any(x => x.ColumnName == field.Alias))
                    {
                        var columnName = _datset.Tables[0].Columns[field.Alias].ColumnName;
                        if (dr[columnName] == DBNull.Value)
                            itemArray.Add(null);
                        else
                            itemArray.Add(dr[columnName]);
                    }
                    else
                        itemArray.Add(null); //Need proper count so all others are null
                }

                newItem.ItemArray = itemArray.ToArray();
                _configuration.retval.RecordList.Add(newItem);
            }
            #endregion

            _configuration.PerfLoadRecords = true;

        }

    }
}