#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;
using System.Threading.Tasks;
using Gravitybox.Datastore.Server.Core.QueryBuilders;

namespace Gravitybox.Datastore.Server.Core
{
    internal static class SqlHelper
    {
        private static string ToParameterName(this FieldDefinition field, int parameterIndex, int? subIndex = null)
        {
            if (subIndex == null)
                return $"@__ff{Utilities.CodeTokenize(field.Name)}{parameterIndex}";
            else
                return $"@__ff{Utilities.CodeTokenize(field.Name)}{parameterIndex}a{subIndex}";
        }

        #region Members

        private const int UpdateStatsThreshold = 15;
        private const int DeleteBlockSize = 10000;
        private const string WITHNOLOCK_TEXT = "WITH (READUNCOMMITTED)";
        internal const string TimestampField = "__Timestamp";
        internal const string RecordIdxField = "__RecordIdx";
        internal const string RecordExistsField = "@_xfound";
        internal const string OldRecordIdxField = "@__oldRecordIdx";
        internal const string HashField = "__hash";
        internal const string FoundField = "__found";
        internal const int SmallRecordBlock = 1000;
        private const string NULLVALUE = "{null}";
        internal const int ThreadTimeout = 60000;
        internal const string EmptyWhereClause = "1=1";

        private static Random _rnd = new Random();
        private static System.Collections.Concurrent.ConcurrentDictionary<Guid, DateTime> _lastUpdatedList = new System.Collections.Concurrent.ConcurrentDictionary<Guid, DateTime>();
        private static Cache<string, RepositorySchema> _updateDataSchemaCache = new Cache<string, RepositorySchema>(new TimeSpan(0, 30, 0), 4391);
        private static System.Collections.Concurrent.ConcurrentDictionary<Guid, DateTime> _childTableRefresh = new System.Collections.Concurrent.ConcurrentDictionary<Guid, DateTime>();
        private static System.Collections.Concurrent.ConcurrentDictionary<Guid, int> _permissionCount = new System.Collections.Concurrent.ConcurrentDictionary<Guid, int>();
        private static bool _isDefragging = false;
        private static System.Timers.Timer _timerUpdateChildTables = null;

        #endregion

        #region Constructor

        static SqlHelper()
        {
            //This will process the Async update where statements
            _timerUpdateChildTables = new System.Timers.Timer(10000);
            _timerUpdateChildTables.Elapsed += _timerUpdateChildTables_Elapsed;
            _timerUpdateChildTables.Start();

        }

        #endregion

        #region Initialize

        internal static void Reset()
        {
            _lastUpdatedList = new System.Collections.Concurrent.ConcurrentDictionary<Guid, DateTime>();
            _updateDataSchemaCache = new Cache<string, RepositorySchema>(new TimeSpan(0, 30, 0), 4391);
            _childTableRefresh = new System.Collections.Concurrent.ConcurrentDictionary<Guid, DateTime>();
            _permissionCount = new System.Collections.Concurrent.ConcurrentDictionary<Guid, int>();
            RepositoryManager.QueryCache.Reset();
        }

        #endregion

        #region Timer_Elapsed

        private static void _timerUpdateChildTables_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            try
            {
                _timerUpdateChildTables.Stop();
                foreach (var item in _childTableRefresh)
                {
                    if (DateTime.Now.Subtract(item.Value).TotalSeconds > 30)
                    {
                        _childTableRefresh.TryRemove(item.Key, out DateTime dt);
                        Guid parentId;
                        using (var context = new DatastoreEntities())
                        {
                            var r = context.Repository.FirstOrDefault(x => x.UniqueKey == item.Key && x.ParentId != null);
                            if (r != null)
                            {
                                r = context.Repository.FirstOrDefault(x => x.RepositoryId == r.ParentId);
                                parentId = r.UniqueKey;
                                SyncChildTables(item.Key, parentId);
                            }
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
                _timerUpdateChildTables.Start();
            }
        }

        #endregion

        #region GetLastModified

        private static bool ModifiedInThreshold(Guid repositoryid, int seconds)
        {
            var thresholdDate = DateTime.Now.AddSeconds(-seconds);
            var v = _lastUpdatedList.FirstOrDefault(x => x.Value > thresholdDate && x.Key == repositoryid);
            if (v.IsDefault()) return false;
            return true;
        }

        #endregion

        #region MarkUpdated

        private static void MarkUpdated(Guid repositoryId)
        {
            try
            {
                if (_lastUpdatedList.TryGetValue(repositoryId, out DateTime lastTime))
                {
                    _lastUpdatedList.TryUpdate(repositoryId, DateTime.Now, lastTime);
                }
                else
                {
                    _lastUpdatedList.TryAdd(repositoryId, DateTime.Now);
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        #endregion

        #region ExecuteSql

        internal static int ExecuteSql(string connectionString, string sql)
        {
            return ExecuteSql(connectionString, sql, new List<SqlParameter>());
        }

        internal static int ExecuteSql(string connectionString, string sql, List<SqlParameter> parameters, bool useTransaction = true, bool retry = false)
        {
            const int MAX_TRY = 1;
            var tryCount = 0;
            var processed = 0;
            if (string.IsNullOrEmpty(sql)) return processed;
            if (parameters == null) parameters = new List<SqlParameter>();
            var timer = Stopwatch.StartNew();
            var errorMsg = new List<string>();
            var timeout = 60;
            do
            {
                try
                {
                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        if (useTransaction)
                        {
                            using (var transaction = connection.BeginTransaction())
                            {
                                var scripts = BreakSqlBlocks(sql);
                                foreach (var sqlBlock in scripts)
                                {
                                    using (var command = connection.CreateCommand())
                                    {
                                        command.Transaction = transaction;
                                        command.CommandTimeout = timeout;
                                        command.CommandText = sqlBlock;
                                        command.CommandType = CommandType.Text;
                                        command.Parameters.AddRange(parameters.ToList().Cast<ICloneable>().ToList().Select(x => x.Clone()).Cast<SqlParameter>().ToArray());
                                        var count = command.ExecuteNonQuery();
                                        if (count > 0) processed += count;
                                    }
                                }
                                transaction.Commit();
                            }
                        }
                        else
                        {
                            var scripts = BreakSqlBlocks(sql);
                            foreach (var sqlBlock in scripts)
                            {
                                using (var command = connection.CreateCommand())
                                {
                                    command.CommandTimeout = timeout;
                                    command.CommandText = sqlBlock;
                                    command.CommandType = CommandType.Text;
                                    command.Parameters.AddRange(parameters.ToList().Cast<ICloneable>().ToList().Select(x => x.Clone()).Cast<SqlParameter>().ToArray());
                                    var count = command.ExecuteNonQuery();
                                    if (count > 0) processed += count;
                                }
                            }
                        }
                    }
                    return processed;
                }
                catch (Exception ex)
                {
                    //Keep track of each try for logging
                    errorMsg.Add($"[Try={tryCount}|Time={timer.ElapsedMilliseconds}]");

                    if (retry && tryCount < MAX_TRY)
                    {
                        if (tryCount < 4)
                            Thread.Sleep(_rnd.Next(80, 200));
                        else if (tryCount < 8)
                            Thread.Sleep(_rnd.Next(500, 1500));
                        else
                            Thread.Sleep(_rnd.Next(2000, 4000));
                        tryCount++;
                    }
                    else
                    {
                        //if (sql.Length > 200) sql = sql.Substring(0, 200) + "...";
                        timer.Stop();
                        //LoggerCQ.LogError(ex, "ExecuteSql: Elapsed=" + timer.ElapsedMilliseconds + ", Retry=" + retry + ", SQL=" + sql);
                        LoggerCQ.LogError(ex, $"ExecuteSql: Elapsed={timer.ElapsedMilliseconds}, Try={retry}, Debug={errorMsg.ToStringList("|")}, Error={ex.Message}");
                        throw;
                    }
                }
            } while (true);
        }

        private static int ExecuteSqlPartial(string sql, List<SqlParameter> parameters, SqlConnection connection, SqlTransaction transaction)
        {
            var processed = 0;
            if (string.IsNullOrEmpty(sql)) return processed;
            if (parameters == null) parameters = new List<SqlParameter>();
            try
            {
                var scripts = BreakSqlBlocks(sql);
                foreach (var sqlBlock in scripts)
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.Transaction = transaction;
                        command.CommandTimeout = 60;
                        command.CommandText = sqlBlock;
                        command.CommandType = CommandType.Text;
                        command.Parameters.AddRange(parameters.ToArray());
                        processed += command.ExecuteNonQuery();
                    }
                }
                return processed;
            }
            catch (SqlException ex)
            {
                if (ex.Message.Contains("Arithmetic overflow error"))
                {
                    var sb = new StringBuilder();
                    sb.AppendLine(sql);
                    if (parameters != null)
                    {
                        foreach (var p in parameters)
                        {
                            if (p.Value == System.DBNull.Value)
                                sb.AppendLine(p.ParameterName + "=NULL");
                            else
                                sb.AppendLine(p.ParameterName + "=" + p.Value.ToString());
                        }
                    }
                    LoggerCQ.LogError(sb.ToString());
                }
                throw;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        internal static DataSet GetDataset(string connectionString, string sql, List<SqlParameter> parameters = null, int timeOut = 60, int maxRetry = 1)
        {
            if (maxRetry < 0) maxRetry = 0;
            var tryCount = 0;
            var timer = Stopwatch.StartNew();

            if (parameters == null) parameters = new List<SqlParameter>();
            if (timeOut < 1) timeOut = 1;
            var errorMsg = new List<string>();

            using (var command = new SqlCommand())
            {
                //Declare outside of try/catch so parameters are only added once to a collection (causes an error if loop more than once)
                command.CommandText = sql;
                command.CommandType = CommandType.Text;
                command.Parameters.AddRange(parameters.ToList().Cast<ICloneable>().ToList().Select(x => x.Clone()).Cast<SqlParameter>().ToArray());
                command.CommandTimeout = timeOut;
                do
                {
                    try
                    {
                        using (var connection = new SqlConnection(connectionString))
                        {
                            connection.Open();
                            command.Connection = connection;
                            var da = new SqlDataAdapter { SelectCommand = command };
                            var ds = new DataSet();
                            da.Fill(ds);
                            return ds;
                        }
                    }
                    catch (Exception ex)
                    {
                        //Keep track of each try for logging
                        errorMsg.Add($"[Try={tryCount}|Elapsed={timer.ElapsedMilliseconds}]");

                        if (tryCount < maxRetry &&
                            !ex.Message.Contains("Invalid column name") &&
                            !ex.Message.Contains("Invalid object name") &&
                            !ex.Message.Contains("Ambiguous column name"))
                        {
                            Thread.Sleep(100);
                            tryCount++;
                        }
                        else if (ex.Message.Contains("Timeout expired") || ex.Message.Contains("Timeout Expired") || ex.Message.Contains("timeout occurred"))
                        {
                            //Do not write out query of timeout
                            timer.Stop();
                            //LoggerCQ.LogError(ex, $"Elapsed={timer.ElapsedMilliseconds}");
                            throw;
                        }
                        else
                        {
                            //if (sql.Length > 200) sql = sql.Substring(0, 200) + "...";
                            timer.Stop();
                            //LoggerCQ.LogError(ex, "GetDataset: Elapsed=" + timer.ElapsedMilliseconds + ", Try=" + tryCount + ", SQL=" + sql);
                            LoggerCQ.LogError(ex, $"GetDataset: Elapsed={timer.ElapsedMilliseconds}, Try={tryCount}, Debug={errorMsg.ToStringList("|")}, Error={ex.Message}");
                            throw;
                        }
                    }
                } while (true);
            }
        }

        private static Cache<long, List<string>> _blockCache = new Cache<long, List<string>>(new TimeSpan(0, 10, 0), 7487);

        private static List<string> BreakSqlBlocks(string sql)
        {
            var hv = EncryptionDomain.HashFast(sql);
            return _blockCache.GetOrAdd(hv, key =>
            {
                var retval = new List<string>();
                var allLines = sql.Replace("\r\n", "\n").Replace("\r", "\n").Split('\n');
                var sb = new StringBuilder();
                foreach (var lineText in allLines)
                {
                    if (lineText.ToUpper().Trim() == "GO")
                    {
                        var s = sb.ToString();
                        s = s.Trim();
                        retval.Add(s);
                        sb = new StringBuilder();
                    }
                    else
                    {
                        var s = lineText;
                        if (s.EndsWith("\r")) s = lineText.Substring(0, lineText.Length - 1);
                        sb.AppendLine(s);
                    }
                }

                //Last string
                var text = sb.ToString();
                if (!string.IsNullOrEmpty(text))
                    retval.Add(text);

                retval = retval.Where(x => x != "").ToList();
                return retval;
            });
        }

        #endregion

        #region RemoveRepository

        public static void RemoveRepository(string connectionString, Guid repositoryKey)
        {
            try
            {
                var sb = new StringBuilder();
                var schema = RepositoryManager.GetSchema(repositoryKey);
                if (schema == null) return;

                var dataTable = GetTableName(schema.ID);
                var dimensionTable = GetDimensionTableName(schema.ID);
                var dimensionValueTable = GetDimensionValueTableName(schema.ID);
                var userPermissionTable = GetUserPermissionTableName(schema.ID);

                if (schema.ParentID != null)
                {
                    var viewName = GetTableViewName(schema.ID);
                    sb.AppendLine($"if exists (select * from sys.objects where name = '{viewName}' and type = 'V')");
                    sb.AppendLine($"drop view [{viewName}]");
                }
                else
                {
                    var childTables = new List<Guid>();
                    if (schema.ParentID == null)
                    {
                        using (var context = new DatastoreEntities(connectionString))
                        {
                            childTables = context.Repository.Where(x => x.ParentId == schema.InternalID).Select(x => x.UniqueKey).ToList();
                            foreach (var gid in childTables)
                            {
                                var viewName = GetTableViewName(gid);
                                sb.AppendLine($"if exists (select * from sys.objects where name = '{viewName}' and type = 'V')");
                                sb.AppendLine($"drop view [{viewName}]");
                            }
                        }
                    }
                }

                //Remove all non-list tables
                RetryHelper.DefaultRetryPolicy(3)
                    .Execute(() =>
                    {
                        sb.AppendLine($"if exists (select * from sys.objects where name = '{dimensionTable}' and type = 'U')");
                        sb.AppendLine($"DROP TABLE [{dimensionTable}]");
                        sb.AppendLine($"if exists (select * from sys.objects where name = '{userPermissionTable}' and type = 'U')");
                        sb.AppendLine($"DROP TABLE [{userPermissionTable}]");
                        sb.AppendLine($"if exists (select * from sys.objects where name = '{dimensionValueTable}' and type = 'U')");
                        sb.AppendLine($"DROP TABLE [{dimensionValueTable}]");
                        sb.AppendLine($"if exists (select * from sys.objects where name = '{dataTable}' and type = 'U')");
                        sb.AppendLine($"DROP TABLE [{dataTable}]");

                        //Run all drop scripts except the List dimension tables
                        ExecuteSql(connectionString, sb.ToString(), null, true);
                    });

                //Remove all the Y-List tables
                RetryHelper.DefaultRetryPolicy(3)
                    .Execute(() =>
                    {
                        sb = new StringBuilder();
                        sb.AppendLine("declare @name varchar(500)");
                        sb.AppendLine($"DECLARE tempCursor CURSOR FOR select name from sys.objects where type = 'U' and  name like 'Y_{schema.ID.ToString()}%'");
                        sb.AppendLine("OPEN tempCursor;");
                        sb.AppendLine("FETCH NEXT FROM tempCursor INTO @name;");
                        sb.AppendLine("WHILE @@FETCH_STATUS = 0");
                        sb.AppendLine("	BEGIN");
                        sb.AppendLine("		exec('if exists (select * from sys.objects where name = ''' + @name + ''' and type = ''U'') DROP TABLE ['+ @name +'];')");
                        sb.AppendLine("		FETCH NEXT FROM tempCursor INTO @name;");
                        sb.AppendLine("	END;");
                        sb.AppendLine("CLOSE tempCursor;");
                        sb.AppendLine("DEALLOCATE tempCursor;");
                        sb.AppendLine();
                        ExecuteSql(connectionString, sb.ToString(), null);
                    });

            }
            catch (Exception ex)
            {
                throw;
            }
        }

        #endregion

        #region AddRepository

        public static bool AddRepository(string connectionString, RepositorySchema fullSchema, string fileGroup)
        {
            try
            {
                if (fullSchema.FieldList.Count != fullSchema.FieldList.Select(x => x.TokenName).Distinct().Count())
                    throw new Exception("All field names must be unique.");

                RepositorySchema schema = null;
                const int NoParentId = -1;
                var parentId = NoParentId;
                if (fullSchema.ParentID != null)
                {
                    using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                    {
                        parentId = context.Repository.Where(x => x.UniqueKey == fullSchema.ParentID).Select(x => x.RepositoryId).FirstOrDefault();
                    }
                }

                #region Format schema as extension if derived from abstract
                Guid parentKey = Guid.Empty;
                if (parentId != NoParentId)
                {
                    using (var context = new DatastoreEntities())
                    {
                        var parentXml = context.Repository.Where(x => x.RepositoryId == parentId).Select(x => x.DefinitionData).FirstOrDefault();
                        if (string.IsNullOrEmpty(parentXml))
                            throw new Exception("Abstract schema not found");

                        var parentSchema = new RepositorySchema();
                        parentSchema.LoadXml(parentXml);
                        parentKey = parentSchema.ID;
                        schema = fullSchema.Subtract(parentSchema);
                    }
                }
                else
                {
                    schema = fullSchema;
                }
                #endregion

                var dataTable = GetTableName(schema);
                var parameters = new List<SqlParameter>();
                var sb = new StringBuilder();

                //Create entry into Repository table
                sb.AppendLine($"if not exists(select * from [Repository] where UniqueKey = '{schema.ID}')");
                sb.AppendLine("BEGIN"); //Wrap whole statement in one big IF statement for transaction
                sb.AppendLine("INSERT INTO [Repository] (UniqueKey, IsInitialized, Name, ItemCount, VersionHash, DefinitionData, ParentId) VALUES (@__z_key, 1, @__z_name, 0, @__z_vh, @__z_dd, @__zpid)");
                sb.AppendLine();
                parameters.Add(new SqlParameter { DbType = DbType.Guid, IsNullable = false, ParameterName = "@__z_key", Value = schema.ID });
                parameters.Add(new SqlParameter { DbType = DbType.String, IsNullable = false, ParameterName = "@__z_name", Value = schema.Name });
                parameters.Add(new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@__z_vh", Value = schema.VersionHash });
                parameters.Add(new SqlParameter { DbType = DbType.String, IsNullable = false, ParameterName = "@__z_dd", Value = fullSchema.ToXml() });
                parameters.Add(new SqlParameter { DbType = DbType.Int32, IsNullable = true, ParameterName = "@__zpid", Value = (parentId == NoParentId ? System.DBNull.Value : (object)parentId) });

                //Create user table
                sb.AppendLine($"--CREATE TABLE [{dataTable}]");
                sb.AppendLine($"if not exists(select * from sys.objects where name = '{dataTable}' and type = 'U')");
                sb.AppendLine($"CREATE TABLE [{dataTable}] (");

                if (fullSchema.ParentID == null)
                    sb.AppendLine($"	[{RecordIdxField}] [BIGINT] IDENTITY (1, 1) NOT NULL");
                else
                    sb.AppendLine($"	[{RecordIdxField}] [BIGINT] NOT NULL");

                sb.AppendLine($"	, [{HashField}] [BIGINT] CONSTRAINT [DF__{dataTable}_{HashField}] DEFAULT 0 NOT NULL");
                sb.AppendLine($"	, [{TimestampField}] INT NOT NULL CONSTRAINT [DF__{dataTable}_{TimestampField}] DEFAULT 0");

                foreach (var column in GetPrimaryTableFields(schema, false))
                {
                    var sqlLength = string.Empty;
                    if (column.DataType == RepositorySchema.DataTypeConstants.String)
                    {
                        if (column.Length > 0) sqlLength = $"({column.Length})";
                        else sqlLength = "(MAX)";
                    }
                    sb.AppendLine($"	, [{column.TokenName}] [{column.ToSqlType()}] " + sqlLength + " " + (column.AllowNull ? "NULL" : "NOT NULL"));
                }

                foreach (var fieldName in schema.DimensionList.Where(x => x.DimensionType != RepositorySchema.DimensionTypeConstants.List).Select(dimension => $"__d{dimension.TokenName}"))
                {
                    sb.AppendLine($"	, [{fieldName}] [BIGINT] NULL");
                }

                //End Create table
                sb.AppendLine(")");

                if (!string.IsNullOrEmpty(fileGroup))
                {
                    sb.AppendLine($"ON [{fileGroup}];");
                }

                sb.AppendLine();

                var hasDataGrouping = schema.FieldList.Any(x => x.IsDataGrouping);
                var indexList = new List<string>();
                #region Add Internal PK
                {
                    //A table with a GetCode columns must has a clustered index
                    var hasGeo = schema.FieldList.Any(x => x.DataType == RepositorySchema.DataTypeConstants.GeoCode);

                    var pkIndexName = "PK_" + dataTable.ToUpper();
                    indexList.Add(pkIndexName);
                    sb.AppendLine($"if not exists(select * from sys.indexes where name = '{pkIndexName}')");
                    sb.AppendLine("BEGIN");

                    var clustered = "CLUSTERED";

                    //If there is a data grouping then PK must be non-clustered so that grouping index can be clustered
                    if (hasDataGrouping)
                        clustered = "NONCLUSTERED";

                    sb.AppendLine($"ALTER TABLE [{dataTable}] WITH NOCHECK ADD CONSTRAINT [{pkIndexName}] PRIMARY KEY {clustered} ([{RecordIdxField}]);");

                    //If PK is non clustered then compress this index
                    if (hasDataGrouping && ConfigHelper.SupportsCompression)
                    {
                        sb.AppendLine($"ALTER INDEX [{pkIndexName}] ON [{dataTable}] REBUILD WITH (DATA_COMPRESSION = PAGE);");
                    }

                    sb.AppendLine("END");
                }
                #endregion

                #region Data grouping
                if (hasDataGrouping)
                {
                    //Determine if there is any data grouping field to add to the primary key before the identity
                    //This will group the data on disk by this field
                    var pkFields = new List<string>();
                    var dgList = schema.FieldList.Where(x => x.IsDataGrouping).ToList();
                    dgList.ForEach(x => pkFields.Add($"[{x.TokenName}]"));
                    pkFields.Add($"[{RecordIdxField}]"); //always end with the identity

                    var indexName = $"DATAGROUP_{dataTable}";
                    indexList.Add(indexName);
                    sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}')");
                    sb.AppendLine($"CREATE CLUSTERED INDEX [{indexName}] ON [{dataTable}] ({pkFields.ToCommaList()});");
                    sb.AppendLine();
                }
                #endregion

                #region Add internally managed indexes
                //Add Hash index
                {
                    var indexName = SqlHelper.GetIndexName(new FieldDefinition { Name = SqlHelper.HashField }, dataTable);
                    sb.AppendLine($"CREATE NONCLUSTERED INDEX [{indexName}] ON [{dataTable}] ([{SqlHelper.HashField}] ASC){GetSqlIndexFileGrouping()};");
                    if (ConfigHelper.SupportsCompression)
                        sb.AppendLine($"ALTER INDEX [{indexName}] ON [{dataTable}] REBUILD WITH (DATA_COMPRESSION = PAGE);");
                }
                //Add Timestamp index
                {
                    var indexName = SqlHelper.GetIndexName(new FieldDefinition { Name = SqlHelper.TimestampField }, dataTable);
                    sb.AppendLine($"CREATE NONCLUSTERED INDEX [{indexName}] ON [{dataTable}] ([{SqlHelper.TimestampField}] ASC){GetSqlIndexFileGrouping()};");
                    if (ConfigHelper.SupportsCompression)
                        sb.AppendLine($"ALTER INDEX [{indexName}] ON [{dataTable}] REBUILD WITH (DATA_COMPRESSION = PAGE);");
                }
                #endregion

                sb.AppendLine("select 1;");
                sb.AppendLine("END");
                sb.AppendLine("ELSE");
                sb.AppendLine("BEGIN");
                sb.AppendLine("select 0");
                sb.AppendLine("END");

                sb.AppendLine(GetDimensionTableCreate(schema.ID));
                sb.AppendLine(GetUserPermissionTableCreate(schema));

                //The query will return "1" if table was created and "0" if it already existed
                var ds = GetDataset(connectionString, sb.ToString(), parameters);
                var wasCreated = false;
                if (ds.Tables.Count == 1 && ds.Tables[0].Rows.Count == 1)
                    wasCreated = (((int)ds.Tables[0].Rows[0][0]) == 1);

                if (wasCreated)
                {
                    //Add all indexes, New transaction, much faster
                    sb = new StringBuilder();
                    sb.Append(GetRepositorySql(schema, indexList));
                    ExecuteSql(connectionString, sb.ToString(), null, false, true);

                    //Create a FTS index for all string columns
                    UpdateFTSIndex(connectionString, schema);

                    //Create List dimension tables
                    foreach (var field in GetNonPrimaryTableFields(schema).Cast<DimensionDefinition>().ToList())
                    {
                        sb = new StringBuilder();
                        var dimensionTable = GetListTableName(schema.ID, field.DIdx);
                        sb.AppendLine(GetListTableCreate(dimensionTable));
                        ExecuteSql(connectionString, sb.ToString(), null);
                    }

                    //Compress table data
                    if (ConfigHelper.SupportsCompression)
                    {
                        sb = new StringBuilder();
                        sb.AppendLine($"if exists(select * from sys.objects where name = '{dataTable}' and type = 'U')");
                        sb.AppendLine($"ALTER TABLE [{dataTable}] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE);");
                        ExecuteSql(connectionString, sb.ToString(), null);
                    }

                    #region Add view for inherited tables
                    if (schema.ParentID != null)
                    {
                        CreateView(schema, fullSchema, connectionString);

                        //Create matching data for all rows in parent table
                        sb = new StringBuilder();
                        sb.AppendLine($"insert into [{GetTableName(schema.ID)}] ({RecordIdxField})");
                        sb.AppendLine($"select a.[{RecordIdxField}] from [{GetTableName(schema.ParentID.Value)}] a left join [{GetTableName(schema.ID)}] b");
                        sb.AppendLine($"on a.[{RecordIdxField}] = b.[{RecordIdxField}] ");
                        sb.AppendLine($"where b.[{RecordIdxField}] IS NULL");

                        ExecuteSql(connectionString, sb.ToString());
                    }
                    #endregion
                }
                else
                {
                    LoggerCQ.LogError($"The repository tables were not created! ID={fullSchema.ID}");
                    throw new Exception($"The repository tables were not created! ID={fullSchema.ID}");
                }

                return true;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        #endregion

        #region SQL Table Changes

        /// <summary>
        /// Delete all indexes that are not generated for this schema
        /// </summary>
        private static void CleanIndexes(string connectionString, RepositorySchema schema, List<string> indexList)
        {
            indexList = indexList.ToList(); //Clone
            indexList.Add("DATAGROUP_" + GetTableName(schema)); //Include the datagrouping is exists

            var dataTable = SqlHelper.GetTableName(schema);

            //Add indexes for timestamp, hash so do not remove
            indexList.Add(SqlHelper.GetIndexName(new FieldDefinition { Name = SqlHelper.TimestampField }, dataTable));
            indexList.Add(SqlHelper.GetIndexName(new FieldDefinition { Name = SqlHelper.HashField }, dataTable));

            var sb = new StringBuilder();
            sb.AppendLine("select name from sys.indexes where object_id = (");
            sb.AppendLine($"select top 1 object_id from sys.objects where name = '{dataTable}' and type = 'U'");
            sb.AppendLine(") and name not like 'PK%'");

            var ds = GetDataset(connectionString, sb.ToString());
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                var name = (string)row["name"];
                if (!indexList.Any(x => x == name))
                {
                    sb = new StringBuilder();
                    sb.AppendLine($"if exists(select * from sys.indexes where name = '{name}')");
                    sb.AppendLine($"DROP INDEX [{name}] ON [{dataTable}]");
                    ExecuteSql(connectionString, sb.ToString());
                }
            }
        }

        internal static string GetRepositorySql(RepositorySchema schema, List<string> indexList = null)
        {
            if (indexList == null) indexList = new List<string>();
            var dataTable = GetTableName(schema);
            var sb = new StringBuilder();

            #region Add Indexes for fields

            //All fields
            foreach (var field in schema.FieldList)
            {
                var dimension = field as DimensionDefinition;

                var useIndex = (field.DataType != RepositorySchema.DataTypeConstants.GeoCode);
                if (field.DataType == RepositorySchema.DataTypeConstants.String && ((field.Length <= 0) || (field.Length > 450)))
                    useIndex = false;

                //Do not create indexes for list indexes
                if (dimension != null && dimension.DimensionType == RepositorySchema.DimensionTypeConstants.List)
                    useIndex = false;

                //If the field has manually been marked as not indexed then skip adding index
                if (!field.AllowIndex)
                    useIndex = false;

                ////Only add indexes to all fields if specified All setting (always PK)
                //if (schema.FieldIndexing != RepositorySchema.FieldIndexingConstants.All && schema.PrimaryKey != field)
                //    useIndex = false;

                if (useIndex)
                {
                    var indexName = GetIndexName(field, dataTable);
                    indexList.Add(indexName);
                    sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}') and exists(select * from sys.objects where name = '{dataTable}' and type = 'U') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '{field.TokenName}' and o.name = '{dataTable}')");
                    sb.AppendLine("BEGIN");

                    var pkAttr = string.Empty;
                    if (field.IsPrimaryKey)
                        pkAttr = "UNIQUE ";

                    sb.Append($"CREATE {pkAttr}NONCLUSTERED INDEX [{indexName}] ON [{dataTable}] ([{field.TokenName}] {(field.SearchAsc ? "ASC" : "DESC")}){GetSqlIndexFileGrouping()}");
                    sb.AppendLine(";");

                    if (ConfigHelper.SupportsCompression)
                        sb.AppendLine($"ALTER INDEX [{indexName}] ON [{dataTable}] REBUILD WITH (DATA_COMPRESSION = PAGE);");

                    sb.AppendLine("END");
                    sb.AppendLine("GO");
                }
            }

            foreach (var field in schema.FieldList.Where(x => x.DataType == RepositorySchema.DataTypeConstants.GeoCode).ToList())
            {
                var indexName = GetIndexName(field, dataTable);
                indexList.Add(indexName);
                sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}')");
                sb.AppendLine("BEGIN");
                sb.AppendLine($"CREATE SPATIAL INDEX [{indexName}] ON [{dataTable}]");
                sb.AppendLine($"([{field.TokenName}])");
                sb.AppendLine("USING GEOGRAPHY_GRID");
                sb.AppendLine("WITH (GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), CELLS_PER_OBJECT= 256)");
                sb.AppendLine("END");
                sb.AppendLine();
            }

            #endregion

            #region Create Dimension Indexes
            foreach (var dimension in schema.DimensionList.Where(x =>
                x.DimensionType != RepositorySchema.DimensionTypeConstants.List &&
                x.DataType != RepositorySchema.DataTypeConstants.GeoCode).ToList())
            {
                var dimensionColumnName = "__d" + dimension.TokenName;
                var indexName = GetIndexName(dimension, dataTable, true);
                indexList.Add(indexName);
                sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}') and exists(select * from sys.objects where name = '{dataTable}' and type = 'U') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '{dimensionColumnName}' and o.name = '{dataTable}')");
                sb.AppendLine("BEGIN");
                sb.Append($"CREATE NONCLUSTERED INDEX [{indexName}] ON [{dataTable}] ([{dimensionColumnName}]){GetSqlIndexFileGrouping()};");

                if (ConfigHelper.SupportsCompression)
                    sb.AppendLine($"ALTER INDEX [{indexName}] ON [{dataTable}] REBUILD WITH (DATA_COMPRESSION = PAGE);");

                sb.AppendLine("END");
                sb.AppendLine("GO");
            }

            //Create pivot column indexes
            var pivotGroups = schema.FieldList.Select(x => x.PivotGroup).Distinct();
            foreach (var groupName in pivotGroups)
            {
                var fieldList = schema.FieldList.Where(x => x.IsPivot && x.PivotGroup == groupName)
                    .Select(x => x.Name)
                    .ToList();

                if (fieldList.Any())
                {
                    var indexName = GetIndexPivotName(fieldList, dataTable);
                    indexList.Add(indexName);
                    sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}') and exists(select * from sys.objects where name = '{dataTable}' and type = 'U')");
                    sb.AppendLine("BEGIN");
                    sb.AppendLine($"CREATE NONCLUSTERED INDEX [{indexName}] ON [{dataTable}] ({fieldList.Select(x => $"[{x}]").ToCommaList()}){GetSqlIndexFileGrouping()};");

                    if (ConfigHelper.SupportsCompression)
                        sb.AppendLine($"ALTER INDEX [{indexName}] ON [{dataTable}] REBUILD WITH (DATA_COMPRESSION = PAGE);");

                    sb.AppendLine("END");
                    sb.AppendLine("GO");
                }
            }

            #endregion

            return sb.ToString();
        }

        private static string GetSqlIndexFileGrouping()
        {
            //Determine if the indexes are in a separate file group by configuration
            //If not get a random group
            var fileGroup = RepositoryManager.GetRandomFileGroup();
            if (ConfigHelper.SetupConfig.HashIndexFileGroup)
                fileGroup = SetupConfig.IndexFileGroup;

            if (!string.IsNullOrEmpty(fileGroup))
                return $" ON [{fileGroup}]";
            return string.Empty;
        }

        #endregion

        #region UpdateSchema

        public static UpdateScheduleResults UpdateSchema(string connectionString, RepositorySchema currentSchema, RepositorySchema newSchema, bool extremeVerify = false)
        {
            var retval = new UpdateScheduleResults();
            var timer = Stopwatch.StartNew();
            try
            {
                //TODO: This logic will not work if a column is moved from a parent to child repo or vice versa
                RepositorySchema parentSchema = null;

                var parentDataTable = string.Empty;
                if (currentSchema.ParentID != null)
                {
                    parentSchema = RepositoryManager.GetSchema(currentSchema.ParentID.Value, true);
                    parentDataTable = GetTableName(parentSchema);
                    currentSchema = currentSchema.Subtract(parentSchema);
                    var tSchema = newSchema.Subtract(parentSchema);
                    newSchema = tSchema;
                }

                //get all current fields and remove those that exist in the new schema
                var changeList = new List<Tuple<FieldDefinition, FieldDefinition>>();

                if (currentSchema == null || newSchema == null)
                {
                    retval.Errors.Add("One or more of the schemas are not defined.");
                    return retval;
                }

                if (currentSchema.ID != newSchema.ID)
                {
                    retval.Errors.Add("The source and target schemas must both have the same key.");
                    return retval;
                }

                if (currentSchema.FieldList == null || newSchema.FieldList == null)
                {
                    retval.Errors.Add("The source and target schemas must both have defined field lists.");
                    return retval;
                }

                #region Loop though old schema and find modified and deleted fields
                foreach (var field in currentSchema.FieldList)
                {
                    if (newSchema.FieldList.Any(x => x.Hash == field.Hash))
                    {
                        //Match do nothing
                    }
                    else if (newSchema.FieldList.Select(x => x.Name).Contains(field.Name))
                    {
                        var newField = newSchema.FieldList.First(x => x.Name == field.Name);
                        if (field.DataType == RepositorySchema.DataTypeConstants.String && newField.DataType == RepositorySchema.DataTypeConstants.String && field.Length != newField.Length)
                        {
                            //Increasing string size
                            changeList.Add(new Tuple<FieldDefinition, FieldDefinition>(field, newField));
                            if (field.AllowTextSearch) retval.FtsChanged = true;
                        }
                        else if (field.FieldType != newField.FieldType)
                        {
                            // Changing field to dimension or vice-versa
                            if (field.DataType == newField.DataType && field.Length == newField.Length)
                            {
                                changeList.Add(new Tuple<FieldDefinition, FieldDefinition>(field, newField));
                                if (field.AllowTextSearch) retval.FtsChanged = true;
                            }
                            else if (field.DataType == RepositorySchema.DataTypeConstants.List)
                            {
                                retval.Errors.Add($"Changing the '{field.Name}' type from a field to dimension or vice versa is not supported for the List datatype.");
                            }
                            else
                            {
                                retval.Errors.Add($"Changing the '{field.Name}' type from a field to dimension or vice versa is not only supported for the same data type and length.");
                            }
                        }
                        else if (field.HashNoFts == newField.HashNoFts)
                        {
                            //The only different is the FTS has changed
                            retval.FtsChanged = true;
                        }
                        else
                        {
                            //Name matches, so this is a field modification (NOT SUPPORTED)
                            retval.Errors.Add($"The field '{field.Name}' has changed which is not supported.");
                        }
                    }
                    else //missing from new schema so removed
                    {
                        changeList.Add(new Tuple<FieldDefinition, FieldDefinition>(field, null));
                        if (field.AllowTextSearch) retval.FtsChanged = true;
                    }
                }
                #endregion

                //Loop though new schema and find new fields
                foreach (var field in newSchema.FieldList)
                {
                    if (currentSchema.FieldList.Any(x => x.Hash == field.Hash))
                    {
                        //Match do nothing
                    }
                    else if (currentSchema.FieldList.Select(x => x.Name).Contains(field.Name))
                    {
                        //Name matches, so this is a field modification (NOT SUPPORTED). Already handled above
                    }
                    else //missing from old schema so added
                    {
                        changeList.Add(new Tuple<FieldDefinition, FieldDefinition>(null, field));
                        if (field.AllowTextSearch) retval.FtsChanged = true;
                    }
                }

                if (retval.Errors.Count > 0) return retval;
                var sb = new StringBuilder();

                //Loop through changes and generate SQL
                foreach (var item in changeList)
                {
                    if (item.Item1 == null && item.Item2 != null)
                    {
                        #region Add column
                        var field = item.Item2;
                        var dataTable = GetTableName(currentSchema);
                        //Always add to schema table no matter if base or inherited
                        //if (parentSchema != null && !parentSchema.FieldList.Any(x => x.Name == field.Name)) dataTable = GetTableName(parentSchema);

                        var length = 0;
                        if (field.DataType == RepositorySchema.DataTypeConstants.String)
                        {
                            length = 100;
                            if (field.Length > 0) length = field.Length;
                        }

                        var definition = field as DimensionDefinition;
                        if (definition != null && definition.DimensionType == RepositorySchema.DimensionTypeConstants.List)
                        {
                            //List field
                            var dimensionTable = GetListTableName(currentSchema.ID, definition.DIdx);
                            sb.AppendLine(GetListTableCreate(dimensionTable));
                        }
                        else
                        {
                            //Non-list field
                            sb.AppendLine($"if exists(select * from sys.objects where name = '{dataTable}' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '{field.TokenName}' and o.name = '{dataTable}')");
                            sb.AppendLine($"ALTER TABLE [{dataTable}] ADD [{field.TokenName}] [" + item.Item2.ToSqlType() + "] " + (length > 0 ? "(" + length + ") " : string.Empty) + "NULL");

                            //If dimension then add DVIDX field
                            if (definition != null)
                            {
                                var dimension = definition;
                                sb.AppendLine($"if exists(select * from sys.objects where name = '{dataTable}' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '__d{definition.TokenName}' and o.name = '{dataTable}')");
                                sb.AppendLine($"ALTER TABLE [{dataTable}] ADD [__d{dimension.TokenName}] [BIGINT] NULL");
                            }
                        }
                        #endregion
                    }
                    else if (item.Item1 != null && item.Item2 == null)
                    {
                        #region Remove column
                        var field = item.Item1;

                        var dataTable = GetTableName(currentSchema);
                        //if (parentSchema != null && !parentSchema.FieldList.Any(x => x.Name == field.Name)) dataTable = GetTableName(parentSchema);
                        var dimension = field as DimensionDefinition;
                        var indexName = GetIndexName(field, dataTable, false);
                        sb.AppendLine($"if exists(select * from sys.indexes where name = '{indexName}')");
                        sb.AppendLine($"DROP INDEX [{indexName}] ON [{dataTable}]");

                        if (dimension != null)
                        {
                            indexName = GetIndexName(field, dataTable, true);
                            sb.AppendLine($"if exists(select * from sys.indexes where name = '{indexName}')");
                            sb.AppendLine($"DROP INDEX [{indexName}] ON [{dataTable}]");
                            sb.AppendLine($"if exists(select * from sys.objects where name = '{dataTable}' and type = 'U') AND exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '__d{field.TokenName}' and o.name = '{dataTable}')");
                            sb.AppendLine($"ALTER TABLE [{dataTable}] DROP COLUMN [__d{field.TokenName}]");

                            //Delete list table
                            if (dimension.DataType == RepositorySchema.DataTypeConstants.List)
                            {
                                sb.AppendLine($"if exists(select * from sys.objects where name = '{GetListTableName(currentSchema.ID, dimension.DIdx)}' and type = 'U')");
                                sb.AppendLine($"DROP TABLE [{GetListTableName(currentSchema.ID, dimension.DIdx)}]");
                            }
                        }

                        sb.AppendLine($"if exists(select * from sys.objects where name = '{dataTable}' and type = 'U') AND exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '{field.TokenName}' and o.name = '{dataTable}')");
                        sb.AppendLine($"ALTER TABLE [{dataTable}] DROP COLUMN [{field.TokenName}]");
                        #endregion
                    }
                    else if (item.Item1 != null && item.Item2 != null)
                    {
                        #region Change column
                        if (item.Item1.DataType == RepositorySchema.DataTypeConstants.String && item.Item2.DataType == RepositorySchema.DataTypeConstants.String && item.Item1.Length != item.Item2.Length)
                        {
                            //The string length has changed
                            var dataTable = GetTableName(currentSchema);
                            if (parentSchema != null && !parentSchema.FieldList.Any(x => x.Name == item.Item2.Name)) dataTable = GetTableName(parentSchema);

                            sb.AppendLine("if exists(select * from sys.objects where name = '" + dataTable +
                                  "' and type = 'U') AND exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '" +
                                  item.Item2.TokenName + "' and o.name = '" + dataTable + "')");

                            var theLength = "max";
                            if (item.Item2.Length > 0)
                                theLength = item.Item2.Length.ToString();

                            //Large strings (>450) cannot be indexed
                            if (item.Item2.Length == 0 || item.Item2.Length > 450 || item.Item2.DataType == RepositorySchema.DataTypeConstants.GeoCode)
                            {
                                var indexName = GetIndexName(item.Item2, dataTable, false);
                                sb.AppendLine($"if exists(select * from sys.indexes where name = '{indexName}')");
                                sb.AppendLine($"DROP INDEX [{indexName}] ON [{dataTable}]");
                            }

                            sb.AppendLine($"ALTER TABLE [{dataTable}] ALTER COLUMN [" + item.Item2.TokenName + "] [" + item.Item2.ToSqlType() + "] (" + theLength + ")");
                        }
                        else if (item.Item1.FieldType != item.Item2.FieldType)
                        {
                            //The type has changed (Field/Dimension)
                            var dataTable = GetTableName(currentSchema);
                            if (parentSchema != null && !parentSchema.FieldList.Any(x => x.Name == item.Item2.Name)) dataTable = GetTableName(parentSchema);

                            //Change from Field->Dimension or Dimension->Field
                            if (item.Item1.FieldType == RepositorySchema.FieldTypeConstants.Field && item.Item2.FieldType == RepositorySchema.FieldTypeConstants.Dimension)
                            {
                                // Field -> Dimension; add DVIDX field
                                var definition = item.Item2 as DimensionDefinition;
                                if (definition != null)
                                {
                                    var dimension = definition;
                                    sb.AppendLine("if exists(select * from sys.objects where name = '" + dataTable +
                                                  "' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '__d" +
                                                  definition.TokenName + "' and o.name = '" + dataTable + "')");
                                    sb.AppendLine($"ALTER TABLE [{dataTable}] ADD [__d{dimension.TokenName}] [BIGINT] NULL");
                                }
                            }

                            if (item.Item1.FieldType == RepositorySchema.FieldTypeConstants.Dimension && item.Item2.FieldType == RepositorySchema.FieldTypeConstants.Field)
                            {
                                // Dimension -> Field; drop DVIDX field
                                var dimension = item.Item1 as DimensionDefinition;
                                if (dimension != null)
                                {
                                    var indexName = GetIndexName(dimension, dataTable, true);
                                    sb.AppendLine($"if exists(select * from sys.indexes where name = '{indexName}')");
                                    sb.AppendLine($"DROP INDEX [{indexName}] ON [{dataTable}]");
                                    sb.AppendLine("if exists(select * from sys.objects where name = '" + dataTable +
                                                  "' and type = 'U') AND exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '__d" + dimension.TokenName +
                                                  "' and o.name = '" + dataTable + "')");
                                    sb.AppendLine($"ALTER TABLE [{dataTable}] DROP COLUMN [__d{dimension.TokenName}]");
                                }
                            }
                        }
                        else
                        {
                            //Change fields not supported. Should never happen since we have handled this above in validation
                            throw new Exception("Unsupported operation.");
                        }
                        #endregion
                    }
                }

                //There is nothing to update so skip out
                retval.HasChanged = false;
                if (sb.ToString() != string.Empty)
                    retval.HasChanged = true;
                if (retval.FtsChanged)
                    retval.HasChanged = true;

                #region Perform the actual SQL changes
                if (retval.HasChanged)
                {
                    var childTables = new List<Guid>();

                    //Remove FTS
                    if (retval.FtsChanged)
                        ExecuteSql(connectionString, GetSqlRemoveFTS(GetTableName(newSchema)), null, false);
                    if (parentSchema != null)
                    {
                        ExecuteSql(connectionString, GetDropView(newSchema.ID));
                        if (retval.FtsChanged)
                            ExecuteSql(connectionString, GetSqlRemoveFTS(GetTableName(parentSchema)), null, false);
                    }
                    else
                    {
                        if (newSchema.ParentID == null)
                        {
                            using (var context = new DatastoreEntities(connectionString))
                            {
                                var tempsb = new StringBuilder();
                                childTables = context.Repository.Where(x => x.ParentId == currentSchema.InternalID).Select(x => x.UniqueKey).ToList();
                                foreach (var gid in childTables)
                                {
                                    tempsb.AppendLine(GetDropView(gid));
                                }
                                ExecuteSql(connectionString, tempsb.ToString());
                            }
                        }
                    }

                    //Get the actually Update SQL
                    var indexList = new List<string>();

                    //Remove all indexes that should not exist
                    if (parentSchema != null)
                    {
                        sb.Append(GetRepositorySql(parentSchema, indexList));
                        sb.Append(GetRepositorySql(newSchema.Subtract(parentSchema), indexList));

                        CleanIndexes(connectionString, parentSchema, indexList);
                        CleanIndexes(connectionString, newSchema.Subtract(parentSchema), indexList);
                    }
                    else
                    {
                        sb.Append(GetRepositorySql(newSchema, indexList));
                        CleanIndexes(connectionString, newSchema, indexList);
                    }

                    //Update the change stamp to clear cache
                    var parameters = new List<SqlParameter>();
                    AddRepositoryChangedSql(newSchema.ID, parameters);

                    //Update the schema
                    ExecuteSql(connectionString, sb.ToString(), parameters, true, true);

                    if (currentSchema.ParentID != null)
                    {
                        RepositoryManager.ClearSchemaCache(currentSchema.ParentID.Value);
                        RepositoryManager.ClearSchemaCache(currentSchema.ID);
                        var fullSchema = RepositoryManager.GetSchema(currentSchema.ParentID.Value);
                        fullSchema.FieldList.AddRange(newSchema.FieldList);
                        CreateView(newSchema, fullSchema, connectionString);
                    }
                    else
                    {
                        //foreach (var gid in childTables)
                        //{
                        //    CreateView(RepositoryManager.GetSchema(gid), newSchema, connectionString);
                        //}
                    }
                }
                #endregion

                #region Update permission field

                sb = new StringBuilder();
                if (currentSchema.UserPermissionField == null && newSchema.UserPermissionField == null)
                {
                    //Do nothing
                    //None before / none now
                    LoggerCQ.LogDebug($"UpdateSchema: ID={newSchema.ID}, Permissions=None");
                }
                else if (currentSchema.UserPermissionField == null && newSchema.UserPermissionField != null)
                {
                    retval.HasChanged = true;
                    //Add new permissions field
                    sb.AppendLine(GetUserPermissionTableCreate(newSchema));
                    ExecuteSql(connectionString, sb.ToString());
                    LoggerCQ.LogDebug($"UpdateSchema: ID={newSchema.ID}, Permissions=Add");
                }
                else if (currentSchema.UserPermissionField != null && newSchema.UserPermissionField == null)
                {
                    retval.HasChanged = true;
                    //Remove existing permissions field
                    var userPermissionTable = GetUserPermissionTableName(newSchema.ID);
                    sb.AppendLine($"if exists (select * from sys.objects where name = '{userPermissionTable}' and type = 'U')");
                    sb.AppendLine($"drop table [{userPermissionTable}]");
                    ExecuteSql(connectionString, sb.ToString());
                    LoggerCQ.LogDebug($"UpdateSchema: ID={newSchema.ID}, Permissions=Remove");
                }
                else if (currentSchema.UserPermissionField.Name != newSchema.UserPermissionField.Name)
                {
                    retval.HasChanged = true;
                    //Changed field
                    var userPermissionTable = GetUserPermissionTableName(newSchema.ID);
                    sb.AppendLine($"if exists (select * from sys.objects where name = '{userPermissionTable}' and type = 'U')");
                    sb.AppendLine($"drop table [{userPermissionTable}]");
                    sb.AppendLine(GetUserPermissionTableCreate(newSchema));
                    ExecuteSql(connectionString, sb.ToString());
                    LoggerCQ.LogDebug($"UpdateSchema: ID={newSchema.ID}, Permissions=Change");
                }
                else if (newSchema.UserPermissionField != null)
                {
                    //Add new permissions field
                    sb.AppendLine(GetUserPermissionTableCreate(newSchema));
                    ExecuteSql(connectionString, sb.ToString());
                    //LoggerCQ.LogDebug($"UpdateSchema: ID={newSchema.ID}, Permissions=JustInCase");
                }

                #endregion

                //In some cases verify the integrity of the repository
                if (extremeVerify)
                {
                    //Create List dimension tables
                    foreach (var field in GetNonPrimaryTableFields(newSchema).Cast<DimensionDefinition>().ToList())
                    {
                        sb = new StringBuilder();
                        var dimensionTable = GetListTableName(newSchema.ID, field.DIdx);
                        sb.AppendLine(GetListTableCreate(dimensionTable));
                        ExecuteSql(connectionString, sb.ToString(), null);
                    }
                }

                if (retval.HasChanged)
                    MarkUpdated(currentSchema.ID);

                timer.Stop();

                return retval;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"UpdateSchema failed: RepositoryId={newSchema.ID}");
                throw;
            }
        }

        private static void AddRepositoryChangedSql(Guid repositoryId, List<SqlParameter> parameters)
        {
            try
            {
                RepositoryManager.SetRepositoryChangeStamp(repositoryId);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        internal static void UpdateFTSIndex(string connectionString, RepositorySchema schema)
        {
            const int maxTry = 3;
            var tries = 0;

            do
            {
                // re-Create a FTS index for all string columns
                try
                {
                    //only create one at a time
                    //lock (_ftsLocker)
                    {
                        var scripts = GetSqlFTS(GetTableName(schema), schema);
                        foreach (var sql in scripts)
                            ExecuteSql(connectionString, sql, null, false);
                        return;
                    }
                }
                catch (SqlException ex)
                {
                    //Ignore if FT index already exists
                    if (ex.Message.StartsWith("A full-text index for table"))
                    {
                        return;
                    }
                    else if (ex.Message.Contains("deadlocked on lock resources"))
                    {
                        tries++;
                        System.Threading.Thread.Sleep(250);
                    }
                    else
                        throw;
                }
                catch (Exception ex)
                {
                    LoggerCQ.LogError(ex, "Failed recreating FTS index.");
                    return;
                }
            } while (tries < maxTry);
        }

        #endregion

        #region UpdateData

        public static UpdateDataSqlResults UpdateData(RepositorySchema schema, List<DimensionItem> dbDimensionList, IEnumerable<DataItem> list, string connectionString)
        {
            const int MaxTry = 5;
            var tryCount = 0;

            var processed = 0;
            var retval = new UpdateDataSqlResults();
            do
            {
                try
                {
                    var dataTable = GetTableName(schema);
                    var parentDataTable = string.Empty;

                    RepositorySchema parentSchema = null;
                    var childTables = new List<Guid>();
                    if (schema.ParentID != null)
                    {
                        parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);
                        schema = schema.Subtract(parentSchema);
                        schema.FieldList.Insert(0, parentSchema.PrimaryKey);
                        parentDataTable = GetTableName(parentSchema);
                    }
                    else
                    {
                        using (var context = new DatastoreEntities(connectionString))
                        {
                            childTables = context.Repository.Where(x => x.ParentId == schema.InternalID).Select(x => x.UniqueKey).ToList();
                        }
                    }

                    var builder = new SqlConnectionStringBuilder(connectionString);
                    builder.ConnectTimeout = 60;
                    connectionString = builder.ToString();

                    using (var connection = new SqlConnection(connectionString))
                    {
                        var fieldIndexPrimaryMap = new Dictionary<string, int>();
                        {
                            var fieldIndex = 0;
                            if (parentSchema != null)
                                parentSchema.FieldList.ForEach(x => fieldIndexPrimaryMap.Add(x.Name, fieldIndex++));
                            schema.FieldList.Where(x => !fieldIndexPrimaryMap.ContainsKey(x.Name)).ToList().ForEach(x => fieldIndexPrimaryMap.Add(x.Name, fieldIndex++));
                        }
                        var pkIndexPrimary = fieldIndexPrimaryMap[schema.PrimaryKey.Name];

                        connection.Open();
                        const int ChunkSize = 20;
                        var chunkStart = 0;
                        var chunkList = list.Skip(chunkStart).Take(ChunkSize).ToList();
                        do
                        {
                            using (var transaction = connection.BeginTransaction())
                            {
                                #region Loop through items
                                string updateSql = null;
                                foreach (var item in chunkList)
                                {
                                    var parameters = new List<SqlParameter>();

                                    #region Hash parameter
                                    //The hash parameter will return with the current has from the database
                                    var hashValue = item.Hash();
                                    var paramHash = new SqlParameter
                                    {
                                        DbType = DbType.Int64,
                                        Direction = ParameterDirection.InputOutput,
                                        IsNullable = false,
                                        ParameterName = "@" + HashField,
                                        Value = hashValue,
                                    };
                                    parameters.Add(paramHash);
                                    #endregion

                                    #region Timestamp parameter
                                    parameters.Add(new SqlParameter
                                    {
                                        DbType = DbType.Int32,
                                        IsNullable = false,
                                        ParameterName = "@" + SqlHelper.TimestampField,
                                        Value = Utilities.CurrentTimestamp,
                                    });
                                    #endregion

                                    #region Found parameter
                                    //The hash parameter will return with the current has from the database
                                    var paramFound = new SqlParameter
                                    {
                                        DbType = DbType.Boolean,
                                        Direction = ParameterDirection.InputOutput,
                                        IsNullable = false,
                                        ParameterName = "@" + FoundField,
                                        Value = false,
                                    };
                                    parameters.Add(paramFound);
                                    #endregion

                                    //For now do not use this optimization. it causes issues
                                    StringBuilder sb = null;
                                    //if (string.IsNullOrEmpty(updateSql))
                                    sb = new StringBuilder();

                                    if (parentSchema != null)
                                        UpdateDataBuildSql(parentSchema, dbDimensionList, parentDataTable, fieldIndexPrimaryMap, pkIndexPrimary, item, null, sb, parameters);
                                    UpdateDataBuildSql(schema, dbDimensionList, dataTable, fieldIndexPrimaryMap, pkIndexPrimary, item, parentDataTable, sb, parameters);

                                    //For now do not use this optimization. it causes issues
                                    //if (sb != null)
                                    updateSql = sb.ToString();

                                    #region Process child tables
                                    //Check if this repository has child tables
                                    //If so then they need to ensure that they have this new record
                                    if (parentSchema == null && childTables.Count > 0)
                                    {
                                        foreach (var childId in childTables)
                                        {
                                            _childTableRefresh.AddOrUpdate(childId, DateTime.Now, (key, value) => DateTime.Now);
                                            //_childTableRefresh.TryAdd(childId, DateTime.Now);
                                            //UpdateChildTables(schema, childId, updateSql, parameters);
                                        }
                                    }
                                    #endregion

                                    processed = ExecuteSqlPartial(updateSql, parameters, connection, transaction);
                                    if (paramHash.Value is System.DBNull)
                                        retval.AffectedCount++;
                                    else if ((long)paramHash.Value != hashValue)
                                        retval.AffectedCount++;

                                    if ((bool)paramFound.Value)
                                        retval.FountCount++;
                                }
                                #endregion

                                //After batch submitted reset the Changestamp
                                if (retval.AffectedCount > 0)
                                {
                                    var parameters = new List<SqlParameter>();
                                    var sb = new StringBuilder();
                                    AddRepositoryChangedSql(schema.ID, parameters);
                                    ExecuteSqlPartial(sb.ToString(), parameters, connection, transaction);
                                }

                                transaction.Commit();
                            }
                            chunkStart += ChunkSize;
                            chunkList = list.Skip(chunkStart).Take(ChunkSize).ToList();
                        } while (chunkList.Count > 0);
                    }

                    //Ignore singles
                    //if (list.Count() >= 10)
                    MarkUpdated(schema.ID);

                    return retval;
                }
                catch (SqlException ex)
                {
                    if (ex.Message.ToLower().Contains("deadlock"))
                    {
                        tryCount++;
                        LoggerCQ.LogWarning(ex, $"UpdateData deadlock: ID={schema.ID}, Try={tryCount}");
                    }
                    else if (ex.Message.Contains("Cannot insert duplicate key row"))
                    {
                        tryCount++;
                        LoggerCQ.LogWarning(ex, $"UpdateData insert duplicate key row: ID={schema.ID}, Try={tryCount}");
                    }
                    else
                        throw;
                    System.Threading.Thread.Sleep(_rnd.Next(200, 800));
                }
                catch (Exception ex)
                {
                    throw;
                }
            } while (tryCount < MaxTry);
            return retval;
        }

        private static void UpdateDataBuildSql(RepositorySchema schema, List<DimensionItem> dbDimensionList, string dataTable, Dictionary<string, int> fieldIndexPrimaryMap,
            int pkIndexPrimary, DataItem item, string parentDataTable, StringBuilder sb, List<SqlParameter> parameters)
        {
            var retval = new UpdateDataSqlResults();
            try
            {
                //Get list if field names+dimension DVIDx fields
                var fieldList = new List<string>();

                var validFieldList = schema.FieldList.Where(x => x.DataType != RepositorySchema.DataTypeConstants.List).ToList();

                //if this is an inherited table then remove the PK since it is not in this table
                if (!string.IsNullOrEmpty(parentDataTable))
                    validFieldList.Remove(schema.PrimaryKey);

                foreach (var field in validFieldList)
                {
                    fieldList.Add($"[{field.TokenName}]");
                    var dimension = field as DimensionDefinition;
                    if (dimension != null)
                    {
                        fieldList.Add($"[__d{dimension.TokenName}]");
                    }
                }

                var valueList = new List<string>();
                var updateValueList = new List<string>();
                foreach (var field in validFieldList)
                {
                    var fieldIndex = fieldIndexPrimaryMap[field.Name];
                    var paramName = $"@field{fieldIndex}";
                    valueList.Add(paramName);
                    updateValueList.Add($"[{field.TokenName}] = {paramName}");
                    object objectValue = null;
                    if (fieldIndex < item.ItemArray.Length)
                        objectValue = item.ItemArray[fieldIndex];

                    var newParameter = CreateParameter(field, fieldIndex, objectValue);
                    if (!parameters.Any(x => x.ParameterName == newParameter.ParameterName))
                        parameters.Add(newParameter);

                    #region Fill the dimension indexed fields

                    {
                        var dimension = field as DimensionDefinition;
                        if (dimension != null)
                        {
                            var dimParam = new SqlParameter
                            {
                                DbType = DbType.Int64,
                                IsNullable = true,
                                ParameterName = $"@dim{fieldIndex}",
                                Value = DBNull.Value
                            };
                            valueList.Add(dimParam.ParameterName);
                            updateValueList.Add($"[__d{field.TokenName}] = {dimParam.ParameterName}");
                            if ((fieldIndex < item.ItemArray.Length) && item.ItemArray[fieldIndex] != null)
                            {
                                var d = dbDimensionList.First(x => x.DIdx == dimension.DIdx);
                                if (dimension.DimensionType == RepositorySchema.DimensionTypeConstants.List)
                                {
                                    throw new Exception("List fields not supported");
                                }
                                else
                                {
                                    if ((dimension.DataType == RepositorySchema.DataTypeConstants.Int || dimension.DataType == RepositorySchema.DataTypeConstants.Int64) && dimension.NumericBreak != null && dimension.NumericBreak > 0)
                                    {
                                        var tv = Convert.ToInt64(item.ItemArray[fieldIndex]);
                                        var scaled = ((tv / dimension.NumericBreak) * dimension.NumericBreak).ToString();
                                        //ToList to fix the collection modified error
                                        //var r = d.RefinementList.ToList().FirstOrDefault(x => x.FieldValue == scaled);
                                        var r = d.RefinementList.FirstOrDefault(x => x.FieldValue == scaled);
                                        if (r != null)
                                            dimParam.Value = r.DVIdx;
                                    }
                                    else
                                    {
                                        var tv = GetTypedDimValue(dimension.DataType, item.ItemArray[fieldIndex]);
                                        //ToList to fix the collection modified error
                                        //var r = d.RefinementList.ToList().FirstOrDefault(x => x.FieldValue == tv);
                                        var r = d.RefinementList.FirstOrDefault(x => x.FieldValue == tv);
                                        if (r != null)
                                            dimParam.Value = r.DVIdx;
                                    }
                                }
                            }
                            parameters.Add(dimParam);
                        }
                    }

                    #endregion

                }

                //If there are no fields to update then skip out
                //If the string builder is not there then no need to rebuild the SQL
                if (!fieldList.Any() || sb == null)
                    return;

                //Determine if has any list dimensions
                var hasListDim = (schema.FieldList
                    .Where(x => x.DataType == RepositorySchema.DataTypeConstants.List &&
                        x is DimensionDefinition)
                        .Cast<DimensionDefinition>()
                    .Count() > 0);

                #region Insert/Update Records

                var pkTableName = dataTable;
                if (!string.IsNullOrEmpty(parentDataTable))
                    pkTableName = parentDataTable;

                if (pkTableName == dataTable)
                {
                    sb.AppendLine($"DECLARE {OldRecordIdxField} INT;");
                    sb.AppendLine("DECLARE @__oldhash BIGINT;");
                    sb.AppendLine("declare @__lastID bigint;");
                }

                sb.AppendLine($"SELECT TOP 1 {OldRecordIdxField} = [{RecordIdxField}], @__oldhash = [{HashField}] FROM [{pkTableName}] WITH (UPDLOCK) WHERE [{schema.PrimaryKey.TokenName}] = @field{pkIndexPrimary};");

                //Determine if this is a new item (does not exist)
                sb.AppendLine($"declare {RecordExistsField} bit = 0;");
                sb.AppendLine($"if ({OldRecordIdxField} is not null) set {RecordExistsField} = 1;");

                //In normal repositories with no parents use the data hash to determine if execution should proceed
                if (schema.ParentID == null)
                {
                    sb.AppendLine($"if (@__oldhash = @{HashField})");
                    sb.AppendLine("BEGIN");
                    sb.AppendLine($"set @{FoundField} = 1;");
                    sb.AppendLine($"UPDATE [{dataTable}] SET [{TimestampField}] = @{TimestampField} WHERE [{RecordIdxField}] = {OldRecordIdxField}");
                    sb.AppendLine("goto TheEndOfTheScript;");
                    sb.AppendLine("END");
                    sb.AppendLine();
                }

                //Need to check the table for existence and so can choose insert/update
                sb.AppendLine($"if ({RecordExistsField} = 0)");

                //Insert code
                sb.AppendLine("BEGIN");
                sb.AppendLine($"INSERT INTO [{dataTable}] (");
                if (!string.IsNullOrEmpty(parentDataTable)) sb.Append($"[{RecordIdxField}], [{TimestampField}], "); //Instance table must set own RecordIdx
                else sb.Append($"[{TimestampField}], ");
                sb.Append($"[{HashField}], ");
                sb.AppendLine(fieldList.ToCommaList());
                sb.AppendLine(") VALUES (");
                if (!string.IsNullOrEmpty(parentDataTable)) sb.AppendLine($"{OldRecordIdxField},"); //Instance table must set own RecordIdx
                sb.Append($"@{TimestampField}, @{HashField},");
                sb.AppendLine(valueList.ToCommaList());
                sb.AppendLine(");");
                sb.AppendLine("set @__lastID = SCOPE_IDENTITY()");
                sb.AppendLine("END");

                sb.AppendLine("ELSE");

                //Update code
                sb.AppendLine("BEGIN");
                sb.AppendLine($"SET @__lastID = {OldRecordIdxField};");
                sb.AppendLine($"SET @{FoundField}=1;");
                sb.AppendLine($"UPDATE [{dataTable}]");
                sb.AppendLine($"SET [{TimestampField}]=@{TimestampField}, [{HashField}]=@{HashField}, {updateValueList.ToCommaList()}");
                if (string.IsNullOrEmpty(parentDataTable))
                    sb.AppendLine($"WHERE [{schema.PrimaryKey.TokenName}] = @field{pkIndexPrimary}");
                else
                    sb.AppendLine($"WHERE [{RecordIdxField}] = {OldRecordIdxField}");
                sb.AppendLine($"SET @{HashField} = @__oldhash;");
                sb.AppendLine("END");

                #endregion

                #region Process List Fields

                if (hasListDim)
                {
                    //Get a list of "List" dimensions
                    var listDimensions = schema.FieldList
                        .Where(x => x.DataType == RepositorySchema.DataTypeConstants.List &&
                            x is DimensionDefinition)
                            .Cast<DimensionDefinition>()
                        .OrderBy(x => x.DIdx) //keeps SQL consistent so can reuse query plan
                        .ToList();

                    var listDimValueIndex = 0;
                    var sbListDimDelete = new StringBuilder();
                    var sbListDimInsert = new StringBuilder();
                    foreach (var dimension in listDimensions)
                    {
                        var listTable = GetListTableName(schema.ID, dimension.DIdx);
                        sbListDimDelete.AppendLine($"DELETE FROM [{listTable}] WHERE [{RecordIdxField}] = {OldRecordIdxField};");
                        var fieldIndex = fieldIndexPrimaryMap[dimension.Name];
                        if ((fieldIndex < item.ItemArray.Length) && item.ItemArray[fieldIndex] != null)
                        {
                            var l = ((string[])item.ItemArray[fieldIndex]).Distinct().ToList();
                            var usedValues = new List<long>();
                            foreach (var tv in l)
                            {
                                //ToList to fix the collection modified error
                                var d = dbDimensionList.First(x => x.DIdx == dimension.DIdx);
                                var r = d.RefinementList.ToList().FirstOrDefault(x => x.FieldValue == tv);
                                if (r != null && !usedValues.Contains(r.DVIdx))
                                {
                                    var rdvidxParam = new SqlParameter
                                    {
                                        DbType = DbType.Int64,
                                        IsNullable = false,
                                        ParameterName = $"@rdvidx{listDimValueIndex}",
                                        Value = r.DVIdx,
                                    };
                                    sbListDimInsert.AppendLine($"INSERT INTO [{listTable}] ([{RecordIdxField}], [DVIdx]) VALUES (@__lastID, {rdvidxParam.ParameterName})");
                                    parameters.Add(rdvidxParam);
                                    usedValues.Add(r.DVIdx);
                                    listDimValueIndex++;
                                }
                            }
                        }
                    }

                    if (listDimensions.Any())
                    {
                        //Do not bother to issue delete statement, if this is a new insert with no previous record
                        sb.AppendLine($"if ({RecordExistsField} = 1)");
                        sb.AppendLine("BEGIN");
                        sb.Append(sbListDimDelete.ToString());
                        sb.AppendLine("END");
                        sb.AppendLine();
                        sb.AppendLine(sbListDimInsert.ToString());
                    }
                }

                #endregion

                sb.AppendLine($"SET @{HashField} = @__oldhash");
                sb.AppendLine("TheEndOfTheScript:");
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        private static void SyncChildTables(Guid childId, Guid parentId)
        {
            var childTable = GetTableName(childId);
            var parentTable = GetTableName(parentId);

            var timer = Stopwatch.StartNew();
            var sb = new StringBuilder();
            sb.AppendLine($"insert into [{childTable}] ([{RecordIdxField}])");
            sb.AppendLine($"select z1.[{RecordIdxField}]");
            sb.AppendLine($"from [" + parentTable + "] z1 left join");
            sb.AppendLine($"[{childTable}] z2 on z1.[{RecordIdxField}] = z2.[{RecordIdxField}]");
            sb.AppendLine($"where z2.[{RecordIdxField}] IS NULL;");
            sb.AppendLine($"delete z2");
            sb.AppendLine($"from ");
            sb.AppendLine($"[{childTable}] z2 left join");
            sb.AppendLine($"[{parentTable}] z1 on z1.[{RecordIdxField}] = [z2].[{RecordIdxField}]");
            sb.AppendLine($"where [z1].[{RecordIdxField}] IS NULL");

            try
            {
                ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), null, true, true);

            }
            catch (Exception ex)
            {
                LoggerCQ.LogWarning(ex, "SyncChildTables Failed");
            }
            timer.Stop();
            LoggerCQ.LogInfo($"FillChildTables: Child={childId}, Parent={parentId}, Elapsed={timer.ElapsedMilliseconds}");
        }

        public static UpdateDataSqlResults UpdateData(RepositorySchema schema, DataQuery query, List<DimensionItem> dimensionList, IEnumerable<DataFieldUpdate> list, string connectionString)
        {
            const int MaxTry = 5;
            var tryCount = 0;
            var retval = new UpdateDataSqlResults();
            do
            {
                try
                {
                    var dataTable = GetTableName(schema);
                    var parentDataTable = string.Empty;
                    var fullSchema = schema;
                    var viewName = string.Empty;

                    RepositorySchema parentSchema = null;
                    if (schema.ParentID != null)
                    {
                        viewName = GetTableViewName(schema.ID);
                        parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);

                        var tempSchema = _updateDataSchemaCache.Get(schema.ID.ToString());
                        if (tempSchema != null && tempSchema.ChangeStamp == schema.ChangeStamp)
                        {
                            //Just use the one from cache
                            schema = tempSchema;
                        }
                        else
                        {
                            //Create the difference schema and cache it
                            var changeStamp = schema.ChangeStamp;
                            schema = schema.Subtract(parentSchema);
                            schema.FieldList.Insert(0, parentSchema.PrimaryKey);
                            schema.ChangeStamp = changeStamp;
                            _updateDataSchemaCache.Add(schema.ID.ToString(), schema);
                        }

                        parentDataTable = GetTableName(parentSchema);
                    }

                    var builder = new SqlConnectionStringBuilder(connectionString);
                    builder.ConnectTimeout = 60;
                    connectionString = builder.ToString();
                    var parameters = new List<SqlParameter>();
                    var whereClause = GetWhereClause(schema, parentSchema, query, dimensionList, parameters);
                    var innerJoinClause = GetInnerJoinClause(schema, parentSchema, query, dimensionList, parameters);

                    #region Hash parameter
                    parameters.Add(new SqlParameter
                    {
                        DbType = DbType.Int64,
                        IsNullable = false,
                        ParameterName = "@" + HashField,
                        Value = 0,
                    });
                    #endregion

                    #region Timestamp parameter
                    parameters.Add(new SqlParameter
                    {
                        DbType = DbType.Int32,
                        IsNullable = false,
                        ParameterName = "@" + TimestampField,
                        Value = Utilities.CurrentTimestamp,
                    });
                    #endregion

                    using (var connection = new SqlConnection(connectionString))
                    {
                        if (list.Count(x => fullSchema.FieldList.Select(z => z.Name).Contains(x.FieldName)) != list.Count())
                            throw new Exception($"Unknown fields: ID={schema.ID}");
                        connection.Open();
                        using (var transaction = connection.BeginTransaction())
                        {
                            //If all non-dimensions and same DB table then run in 1 query
                            var singleRun = true;
                            var tableCheckList = new List<string>();
                            foreach (var item in list)
                            {
                                var field = fullSchema.FieldList.First(x => x.Name == item.FieldName);
                                var localTable = dataTable;
                                if (parentSchema != null && parentSchema.FieldList.Any(x => x.Name == item.FieldName))
                                    localTable = parentDataTable;
                                tableCheckList.Add(localTable);
                            }
                            singleRun = (tableCheckList.Distinct().Count() == 1 && list.Count(x => x is DimensionDefinition) == 0 && list.Count() > 0);

                            var sb = new StringBuilder();
                            if (singleRun)
                            {
                                #region This can be optimized to run as one query to update multiple fields
                                var localTable = tableCheckList.First();
                                if (!string.IsNullOrEmpty(viewName))
                                {
                                    sb.AppendLine($"UPDATE [{localTable}] SET ");
                                    var index = 0;
                                    foreach (var item in list)
                                    {
                                        var field = fullSchema.FieldList.First(x => x.Name == item.FieldName);
                                        //If this is a dimension field then update the DIDX as well!!!
                                        var dField = field as DimensionDefinition;
                                        if (dField != null)
                                        {
                                            var dItem = dimensionList.FirstOrDefault(x => x.DIdx == dField.DIdx);
                                            if (dItem != null)
                                            {
                                                if (item.FieldValue == null)
                                                {
                                                    sb.AppendLine($"[{localTable}].[__d{Utilities.DbTokenize(field.Name)}] = NULL, ");
                                                }
                                                else
                                                {
                                                    var rValue = dItem.RefinementList.Where(x => x.FieldValue == item.FieldValue.ToString()).Select(x => x.DVIdx).FirstOrDefault();
                                                    var dparam = CreateParameter(field, index, rValue, true);
                                                    parameters.Add(dparam);
                                                    sb.AppendLine($"[{localTable}].[__d{Utilities.DbTokenize(field.Name)}] = {dparam.ParameterName}, ");
                                                }
                                            }
                                        }
                                        var param = CreateParameter(field, index, item.FieldValue);
                                        parameters.Add(param);
                                        sb.AppendLine($"[{localTable}].[" + field.TokenName + "] = " + param.ParameterName + ", ");
                                        index++;
                                    }
                                    sb.AppendLine($"[{localTable}].[{TimestampField}] = @{TimestampField},");
                                    sb.AppendLine($"[{localTable}].[{HashField}] = @{HashField}");
                                    sb.AppendLine($"FROM [{localTable}] A {NoLockText()} {innerJoinClause}");
                                    sb.AppendLine($"inner join [{viewName}] Z on A.{RecordIdxField} = Z.{RecordIdxField}");
                                    sb.AppendLine($"WHERE {whereClause}");
                                }
                                else
                                {
                                    sb.AppendLine("UPDATE Z SET ");
                                    var index = 0;
                                    foreach (var item in list)
                                    {
                                        var field = fullSchema.FieldList.First(x => x.Name == item.FieldName);
                                        //If this is a dimension field then update the DIDX as well!!!
                                        var dField = field as DimensionDefinition;
                                        if (dField != null)
                                        {
                                            var dItem = dimensionList.FirstOrDefault(x => x.DIdx == dField.DIdx);
                                            if (dItem != null)
                                            {
                                                if (item.FieldValue == null)
                                                {
                                                    sb.AppendLine("[Z].[__d" + Utilities.DbTokenize(field.Name) + "] = NULL, ");
                                                }
                                                else
                                                {
                                                    var v = item.FieldValue.ToString();
                                                    if (dField.DataType == RepositorySchema.DataTypeConstants.Bool)
                                                        v = v.ToLower();

                                                    var rValue = dItem.RefinementList.Where(x => x.FieldValue == v).Select(x => x.DVIdx).FirstOrDefault();
                                                    var dparam = CreateParameter(field, index, rValue, true);
                                                    parameters.Add(dparam);
                                                    sb.AppendLine("[Z].[__d" + Utilities.DbTokenize(field.Name) + "] = " + dparam.ParameterName + ", ");
                                                }
                                            }
                                        }
                                        var param = CreateParameter(field, index, item.FieldValue);
                                        parameters.Add(param);
                                        sb.AppendLine($"[Z].[{field.TokenName}] = {param.ParameterName}, ");
                                        index++;
                                    }
                                    sb.AppendLine($"[Z].[{TimestampField}] = @{TimestampField},");
                                    sb.AppendLine($"[Z].[{HashField}] = @{HashField}");
                                    sb.AppendLine($"FROM [{localTable}] Z {NoLockText()} {innerJoinClause}");
                                    sb.AppendLine($"WHERE {whereClause}");
                                }
                                #endregion
                            }
                            else
                            {
                                var index = 0;
                                foreach (var item in list)
                                {
                                    var field = fullSchema.FieldList.First(x => x.Name == item.FieldName);
                                    var localTable = dataTable;
                                    if (parentSchema != null && parentSchema.FieldList.Any(x => x.Name == item.FieldName))
                                        localTable = parentDataTable;
                                    if (field is DimensionDefinition)
                                    {
                                        var dimension = field as DimensionDefinition;
                                        if (dimension.DimensionType == RepositorySchema.DimensionTypeConstants.List)
                                        {
                                            //TODO: Implement list dimension updates
                                            throw new Exception("Update of List dimensions not supported.");
                                        }
                                        else
                                        {
                                            //Normal dimension
                                            var dimensionColumnName = $"__d{dimension.TokenName}";
                                            if (item.FieldValue == null)
                                            {
                                                if (!string.IsNullOrEmpty(viewName))
                                                {
                                                    sb.AppendLine($"UPDATE [{localTable}]");
                                                    sb.AppendLine($"SET [{localTable}].[{field.TokenName}] = NULL, [{localTable}].[{dimensionColumnName}] = NULL, [{localTable}].[{TimestampField}] = @{TimestampField}, [{localTable}].[{HashField}] = @{HashField}");
                                                    sb.AppendLine($"FROM [{localTable}] A {NoLockText()} {innerJoinClause}");
                                                    sb.AppendLine($"inner join [{viewName}] Z on A.{RecordIdxField} = Z.{RecordIdxField}");
                                                    sb.AppendLine($"WHERE {whereClause}");
                                                }
                                                else
                                                {
                                                    sb.AppendLine("UPDATE Z");
                                                    sb.AppendLine($"SET [Z].[{field.TokenName}] = NULL, [Z].[{dimensionColumnName}] = NULL, [Z].[{TimestampField}] = @{TimestampField}, [Z].[{HashField}] = @{HashField}");
                                                    sb.AppendLine($"FROM [{localTable}] Z {NoLockText()} {innerJoinClause}");
                                                    sb.AppendLine($"WHERE {whereClause}");
                                                }
                                            }
                                            else
                                            {
                                                var valueParam = CreateParameter(field, index, item.FieldValue);
                                                parameters.Add(valueParam);
                                                index++;
                                                var v = dimensionList.Where(x => x.DIdx == dimension.DIdx).SelectMany(x => x.RefinementList).FirstOrDefault(x => x.FieldValue.Match(item.FieldValue.ToString()));
                                                if (v == null)
                                                    LoggerCQ.LogWarning($"0x1652: Value not found: Value={item.FieldValue.ToString()}, DIdx={dimension.DIdx}, DimensionName={dimension.Name}");
                                                else
                                                {
                                                    var indexParam = CreateParameter(field, index, v.DVIdx, true);
                                                    parameters.Add(indexParam);

                                                    if (!string.IsNullOrEmpty(viewName))
                                                    {
                                                        sb.AppendLine($"UPDATE [{localTable}]");
                                                        sb.AppendLine($"SET [{localTable}].[{field.TokenName}] = {valueParam.ParameterName}, [{localTable}].[{dimensionColumnName}] = {indexParam.ParameterName}, [{localTable}].[{TimestampField}] = @{TimestampField}, [{localTable}].[{HashField}] = @{HashField}");
                                                        sb.AppendLine($"FROM [{localTable}] A {NoLockText()} {innerJoinClause}");
                                                        sb.AppendLine($"inner join [{viewName}] Z on A.{RecordIdxField} = Z.{RecordIdxField}");
                                                        sb.AppendLine($"WHERE {whereClause}");
                                                    }
                                                    else
                                                    {
                                                        sb.AppendLine("UPDATE Z");
                                                        sb.AppendLine($"SET [Z].[{field.TokenName}] = {valueParam.ParameterName}, [Z].[{dimensionColumnName}] = {indexParam.ParameterName}, [Z].[{TimestampField}] = @{TimestampField}, [Z].[{HashField}] = @{HashField}");
                                                        sb.AppendLine($"FROM [{localTable}] Z {NoLockText()} {innerJoinClause}");
                                                        sb.AppendLine($"WHERE {whereClause}");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        //Normal fields
                                        var param = CreateParameter(field, index, item.FieldValue);
                                        parameters.Add(param);
                                        if (!string.IsNullOrEmpty(viewName))
                                        {
                                            sb.AppendLine($"UPDATE [{localTable}]");
                                            sb.AppendLine($"SET [{localTable}].[{field.TokenName}] = {param.ParameterName}, [{localTable}].[{TimestampField}] = @{TimestampField}, [{localTable}].[{HashField}] = @{HashField}");
                                            sb.AppendLine($"FROM [{localTable}] A {NoLockText()} {innerJoinClause}");
                                            sb.AppendLine($"inner join [{viewName}] Z on A.{RecordIdxField} = Z.{RecordIdxField}");
                                            sb.AppendLine($"WHERE {whereClause}");
                                        }
                                        else
                                        {
                                            sb.AppendLine($"UPDATE Z");
                                            sb.AppendLine($"SET [Z].[{field.TokenName}] = {param.ParameterName}, [Z].[{TimestampField}] = @{TimestampField}, [Z].[{HashField}] = @{HashField}");
                                            sb.AppendLine($"FROM [{localTable}] Z {NoLockText()} {innerJoinClause}");
                                            sb.AppendLine($"WHERE {whereClause}");
                                        }
                                    }

                                    index++;
                                }
                            }

                            retval.AffectedCount = ExecuteSqlPartial(sb.ToString(), parameters, connection, transaction);

                            //After batch submitted reset the Changestamp
                            if (retval.AffectedCount > 0)
                            {
                                parameters = new List<SqlParameter>();
                                var sb1 = new StringBuilder();
                                AddRepositoryChangedSql(schema.ID, parameters);
                                ExecuteSqlPartial(sb1.ToString(), parameters, connection, transaction);
                            }

                            transaction.Commit();
                        }

                        MarkUpdated(schema.ID);
                    }

                    // TODO: Confirm why we need to do a schema health check after a *successful* update
                    //RepositoryHealthMonitor.HealthCheck(schema.ID);
                    return retval;
                }
                catch (SqlException ex)
                {
                    if (ex.Message.ToLower().Contains("deadlock"))
                    {
                        tryCount++;
                        LoggerCQ.LogWarning(ex, $"UpdateData deadlock: ID={schema.ID}, Try={tryCount}");
                    }
                    else
                        throw;
                    System.Threading.Thread.Sleep(_rnd.Next(200, 800));
                }
                catch (Exception ex)
                {
                    throw;
                }
            } while (tryCount < MaxTry);
            return retval;
        }

        #endregion

        #region CreateParameter

        private static SqlParameter CreateParameter(FieldDefinition field, int fieldIndex, object objectValue, bool dimensionIndex = false)
        {
            try
            {
                var paramName = $"@field{fieldIndex}";
                SqlParameter newParam = null;

                if (dimensionIndex)
                {
                    paramName = $"@dfield{fieldIndex}";
                    newParam = new SqlParameter
                    {
                        DbType = DbType.Int64,
                        IsNullable = true,
                        ParameterName = paramName,
                        Value = (long)objectValue
                    };
                }
                else
                {
                    switch (field.DataType)
                    {
                        case RepositorySchema.DataTypeConstants.Bool:
                            newParam = new SqlParameter
                            {
                                DbType = DbType.Boolean,
                                IsNullable = field.AllowNull,
                                ParameterName = paramName,
                            };
                            if (field.AllowNull && objectValue == null) newParam.Value = null;
                            else newParam.Value = Convert.ToBoolean(objectValue);
                            break;
                        case RepositorySchema.DataTypeConstants.DateTime:
                            // Dynamic values ("ExtraValues") typed as DateTime in the schema will still come in as a string.
                            // You can't blind-cast a string as a DateTime.
                            DateTime? dateTimeValue;
                            DateTime dateTimeParsed;
                            if (DateTime.TryParse(objectValue != null ? objectValue.ToString() : string.Empty, out dateTimeParsed))
                                dateTimeValue = dateTimeParsed;
                            else
                                dateTimeValue = null;

                            newParam = new SqlParameter
                            {
                                DbType = DbType.DateTime,
                                IsNullable = field.AllowNull,
                                ParameterName = paramName,
                                Value = dateTimeValue
                            };
                            break;
                        case RepositorySchema.DataTypeConstants.Float:
                            newParam = new SqlParameter
                            {
                                DbType = DbType.Double,
                                IsNullable = field.AllowNull,
                                ParameterName = paramName,
                            };
                            if (field.AllowNull && objectValue == null) newParam.Value = null;
                            else newParam.Value = Convert.ToDouble(objectValue);
                            break;
                        case RepositorySchema.DataTypeConstants.GeoCode:
                            newParam = new SqlParameter(paramName, null);
                            if (objectValue != null)
                            {
                                var v = (GeoCode)objectValue;
                                var geographyBuilder = new Microsoft.SqlServer.Types.SqlGeographyBuilder();
                                geographyBuilder.SetSrid(4326);
                                geographyBuilder.BeginGeography(Microsoft.SqlServer.Types.OpenGisGeographyType.Point);
                                geographyBuilder.BeginFigure(v.Latitude, v.Longitude);
                                geographyBuilder.EndFigure();
                                geographyBuilder.EndGeography();
                                newParam.Value = geographyBuilder.ConstructedGeography;
                                newParam.UdtTypeName = "Geography";
                            }
                            break;
                        case RepositorySchema.DataTypeConstants.Int:
                            newParam = new SqlParameter
                            {
                                DbType = DbType.Int32,
                                IsNullable = field.AllowNull,
                                ParameterName = paramName,
                            };
                            if (field.AllowNull && objectValue == null) newParam.Value = null;
                            else newParam.Value = Convert.ToInt32(objectValue);
                            break;
                        case RepositorySchema.DataTypeConstants.Int64:
                            newParam = new SqlParameter
                            {
                                DbType = DbType.Int64,
                                IsNullable = field.AllowNull,
                                ParameterName = paramName,
                            };
                            if (field.AllowNull && objectValue == null) newParam.Value = null;
                            else newParam.Value = Convert.ToInt64(objectValue);
                            break;
                        case RepositorySchema.DataTypeConstants.String:
                            newParam = new SqlParameter
                            {
                                DbType = DbType.String,
                                IsNullable = field.AllowNull,
                                ParameterName = paramName,
                                Size = (field.Length <= 0 ? 1048576 : field.Length), //1M max
                            };
                            if (field.AllowNull && objectValue == null) newParam.Value = null;
                            else newParam.Value = (string)objectValue ?? string.Empty;
                            if (field.Length > 0) newParam.Size = field.Length;
                            break;
                        case RepositorySchema.DataTypeConstants.List:
                            //Do nothing, handled separately
                            newParam = new SqlParameter(paramName, null);
                            break;
                        default:
                            throw new Exception("Unknown data type!");
                    }
                }

                if (newParam.Value == null) newParam.Value = DBNull.Value;
                else if (!dimensionIndex && field.DataType == RepositorySchema.DataTypeConstants.String && field.Length > 0)
                {
                    if (((string)newParam.Value).Length > field.Length)
                        newParam.Value = ((string)newParam.Value).Substring(0, field.Length);
                }

                return newParam;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"SpecifiedType={field.DataType.ToString()}, ActualType={(objectValue == null ? "NULL" : objectValue.GetType().ToString())}, FieldName={field.Name}");
                throw;
            }
        }

        #endregion

        #region MarkDimensionsChanged

        public static void MarkDimensionsChanged(int repositoryId)
        {
            try
            {
                RepositoryManager.SetDimensionChanged(repositoryId);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        #endregion

        #region Query

        public static DataQueryResults Query(RepositorySchema schema, int repositoryId, DataQuery query, List<DimensionItem> dimensionList)
        {
            return Query(schema, repositoryId, query, dimensionList, out string executeHistory);
        }

        public static DataQueryResults Query(RepositorySchema schema, int repositoryId, DataQuery query, List<DimensionItem> dimensionList, out string executeHistory, int extraRecords = 0)
        {
            try
            {
                var timer = Stopwatch.StartNew();

                var configuration = new ObjectConfiguration
                {
                    query = query,
                    dimensionList = dimensionList,
                    schema = schema,
                    extraRecords = extraRecords,
                    repositoryId = repositoryId,
                };

                //Set the CustomSelect before anything else happens
                if (configuration.IsGrouped)
                    configuration.usingCustomSelect = ObjectConfiguration.SelectionMode.Grouping;
                else if (configuration.query.FieldSelects?.Count > 0)
                    configuration.usingCustomSelect = ObjectConfiguration.SelectionMode.Custom;

                #region Check if specified ALL refinements in a dimension
                //Check if specified ALL refinements in a dimension and if so remove all 
                //since this is functionally eq to specifing none (having no dimension filter)
                if (query.DimensionValueList.Count > 2) //Do not bother to check unless >2 items in list
                {
                    var timer2 = Stopwatch.StartNew();
                    var checkDims = dimensionList
                        .Where(x => x.RefinementList.Count <= query.DimensionValueList.Count)
                        .ToList();
                    foreach (var dItem in checkDims)
                    {
                        var rValues = dItem.RefinementList.Select(x => x.DVIdx).ToList();
                        var c1 = rValues.Intersect(query.DimensionValueList).Count();
                        if (c1 == dItem.RefinementList.Count) query.DimensionValueList.RemoveAll(x => rValues.Contains(x));
                        if (!query.DimensionValueList.Any()) break; //no need to continue this check
                    }
                    timer2.Stop();
                    if (timer2.ElapsedMilliseconds > 5)
                        LoggerCQ.LogDebug("Check Condition 8008:Elapsed=" + timer2.ElapsedMilliseconds);
                }
                #endregion

                if (query.SkipDimensions == null)
                    query.SkipDimensions = new List<long>();

                configuration.dataTable = GetTableName(schema);
                if (schema.ParentID != null)
                    configuration.dataTable = GetTableViewName(schema.ID);

                configuration.dimensionTable = GetDimensionTableName(schema.ID);
                configuration.dimensionValueTable = GetDimensionValueTableName(schema.ID);
                configuration.dimensionTableParent = string.Empty;
                configuration.dimensionValueTableParent = string.Empty;

                if (schema.ParentID != null)
                {
                    configuration.parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);
                    configuration.dimensionTableParent = SqlHelper.GetDimensionTableName(schema.ParentID.Value);
                    configuration.dimensionValueTableParent = SqlHelper.GetDimensionValueTableName(schema.ParentID.Value);
                }

                configuration.orderByClause = "[Z].[" + RecordIdxField + "] ASC";
                var listDim = schema.DimensionList.Where(x => x.DimensionType == RepositorySchema.DimensionTypeConstants.List).Select(x => x.DIdx).ToList();
                //TODO also consider if FieldFilters contain List
                configuration.hasFilteredListDims = dimensionList.Where(x => listDim.Contains(x.DIdx)).SelectMany(x => x.RefinementList).Select(x => x.DVIdx).ToList().Any(x => query.DimensionValueList.Contains(x));
                configuration.parameters = new List<SqlParameter>();
                configuration.whereClause = GetWhereClause(schema, configuration.parentSchema, query, dimensionList, configuration.parameters);
                configuration.innerJoinClause = GetInnerJoinClause(schema, configuration.parentSchema, query, dimensionList, configuration.parameters);
                configuration.normalFields = schema.FieldList.Where(x => x.DataType != RepositorySchema.DataTypeConstants.List).ToList();

                configuration.nonListDimensionDefs = configuration.schema.DimensionList
                    .Where(x => x.DataType != RepositorySchema.DataTypeConstants.List && !configuration.query.SkipDimensions.Contains(x.DIdx))
                    .ToList();

                #region Order By

                configuration.orderByColumns = new List<string>();
                configuration.orderByFields = new List<FieldDefinition>();
                if (query.FieldSorts != null && query.FieldSorts.Any())
                {
                    var usedFields = new List<string>();
                    configuration.orderByClause = string.Empty;
                    foreach (var sf in query.FieldSorts)
                    {
                        //Do not allow duplicates
                        if (!usedFields.Any(x => x == sf.Name))
                        {
                            if (schema.FieldList.Any(x => x.DataType == RepositorySchema.DataTypeConstants.List && x.Name == sf.Name))
                            {
                                //Just in case this comes in do not throw error
                                //throw new Exception("Invalid sort by '" + sf.Name + "' column");
                            }
                            else if (sf.Name == TimestampField)
                            {
                                configuration.orderByClause += $"[Z].[{TimestampField}] {sf.ToSqlDirection()}, ";
                                configuration.orderByColumns.Add($"[{TimestampField}]");
                                configuration.orderByFields.Add(new FieldDefinition { DataType = RepositorySchema.DataTypeConstants.Int, Name = TimestampField });
                            }
                            else if (schema.FieldList.Any(x => x.Name.Match(sf.Name))) //Ensure this is a valid field
                            {
                                var field = schema.FieldList.First(x => x.Name.Match(sf.Name));
                                var sfToken = $"[{sf.TokenName}]";
                                if (field != null &&
                                    !configuration.orderByFields.Contains(field) &&
                                    !configuration.orderByColumns.Any(x => x.Match(sfToken)))
                                {
                                    configuration.orderByClause += $"[Z].[{sf.TokenName}] {sf.ToSqlDirection()}, ";
                                    configuration.orderByColumns.Add(sfToken);
                                    configuration.orderByFields.Add(field);

                                    #region Log warning for no index if need be
                                    if (!field.AllowIndex &&
                                        field.DataType != RepositorySchema.DataTypeConstants.GeoCode &&
                                        field.DataType != RepositorySchema.DataTypeConstants.List &&
                                        field.Name != HashField &&
                                        field.Name != TimestampField)
                                        LoggerCQ.LogTrace($"OrderField NoIndex: ID={schema.ID}, Field={field.Name}");
                                    #endregion

                                }
                            }
                            usedFields.Add(sf.Name);
                        }
                    }
                    configuration.orderByClause += "[Z].[" + RecordIdxField + "] ASC";
                }

                #endregion

                #region Load List Dimensions

                var ldThreads = new List<ListDimensionBuilder>();
                //TODO: This takes a lot of time ~1%, try to cache somehow
                var listDimensions = schema.FieldList
                    .Where(x => x.DataType == RepositorySchema.DataTypeConstants.List && configuration.schema.FieldList.Contains(x))
                    .Cast<DimensionDefinition>()
                    .Where(x => !query.SkipDimensions.Contains(x.DIdx))
                    .ToList();

                //No matter value of IncludeDimensions, you have to query these for the actual records
                var customSelectContainsListFields = configuration.query.FieldSelects != null && listDimensions.Select(x => x.Name).Intersect(configuration.query.FieldSelects.Select(x => x)).Any();
                if (listDimensions.Any() && configuration.usingCustomSelect != ObjectConfiguration.SelectionMode.Grouping)
                {
                    if (configuration.usingCustomSelect == ObjectConfiguration.SelectionMode.Normal || (configuration.usingCustomSelect == ObjectConfiguration.SelectionMode.Custom && customSelectContainsListFields))
                    {
                        //TODO: this takes too long, there is a lot of looping
                        foreach (var newDimension in schema.DimensionList
                            .Where(x => x.DataType == RepositorySchema.DataTypeConstants.List && !query.SkipDimensions.Contains(x.DIdx))
                            .Select(dimension => new DimensionItem
                            {
                                DIdx = dimension.DIdx,
                                Name = dimension.Name,
                                Sortable = false,
                                NumericBreak = dimension.NumericBreak,
                            }))
                        {
                            //This will async build one list dimension
                            ldThreads.Add(new ListDimensionBuilder(configuration, newDimension));
                        }
                    }
                }

                #endregion

                var taskRB = new RecordBuilder(configuration);
                var taskCount = new CountBuilder(configuration);
                var taskDNL = new NormalDimensionBuilder(configuration);
                var taskAgg = new AggregateBuilder(configuration);

                //Record SQL must be created first because it create parameters
                var threadSuccess = true;
                threadSuccess &= taskRB.GenerateSql().Wait(SqlHelper.ThreadTimeout);
                threadSuccess &= taskDNL.GenerateSql().Wait(SqlHelper.ThreadTimeout);
                if (!threadSuccess)
                    throw new DatastoreException("0x2290");

                //Other SQL can be multi-threaded
                var taskList = new List<Task>();
                taskList.Add(taskCount.GenerateSql());
                taskList.Add(taskAgg.GenerateSql());
                foreach (var item in ldThreads)
                    taskList.Add(item.GenerateSql());
                threadSuccess &= Task.WaitAll(taskList.ToArray(), SqlHelper.ThreadTimeout);
                if (!threadSuccess)
                    throw new DatastoreException("0x2291");

                //Execution can be done simultaneously
                taskList.Clear();
                taskList.Add(taskRB.Execute());
                taskList.Add(taskDNL.Execute());
                taskList.Add(taskCount.Execute());
                taskList.Add(taskAgg.Execute());
                foreach (var item in ldThreads)
                    taskList.Add(item.Execute());
                threadSuccess &= Task.WaitAll(taskList.ToArray(), SqlHelper.ThreadTimeout);
                if (!threadSuccess)
                    throw new DatastoreException("0x2292");

                //Loading can be done simultaneously
                threadSuccess &= taskRB.Load().Wait(SqlHelper.ThreadTimeout); //must be loaded first
                if (!threadSuccess)
                    throw new DatastoreException("0x2293");

                taskList.Clear();
                taskList.Add(taskDNL.Load());
                taskList.Add(taskCount.Load());
                taskList.Add(taskAgg.Load());
                foreach (var item in ldThreads)
                    taskList.Add(item.Load());
                threadSuccess &= Task.WaitAll(taskList.ToArray(), SqlHelper.ThreadTimeout);

                if (!threadSuccess)
                    throw new DatastoreException("0x2294");

                #region Setup Dimension Parents

                foreach (var dimension in schema.DimensionList.Where(x => !string.IsNullOrEmpty(x.Parent)).ToList())
                {
                    var d = configuration.retval.DimensionList.FirstOrDefault(x => x.Name == dimension.Name);
                    if (d != null)
                        d.Parent = configuration.retval.DimensionList.FirstOrDefault(x => x.Name == dimension.Parent);
                }

                #endregion

                #region Clean Dimensions

                //If we do not want to cull anything then re-add all the dimension that did not match anything
                if (query.IncludeEmptyDimensions)
                {
                    foreach (var dimension in dimensionList)
                    {
                        var existing = configuration.retval.DimensionList.FirstOrDefault(x => x.DIdx == dimension.DIdx);
                        if (existing == null)
                            configuration.retval.DimensionList.Add((DimensionItem)((System.ICloneable)dimension).Clone());
                        else //the dimension exists so just add all missing refinements
                        {
                            var rl = dimension.RefinementList
                                .Where(x => !existing.RefinementList
                                        .Select(z => z.DVIdx)
                                        .Contains(x.DVIdx))
                                .ToList();
                            rl.ForEach(x => existing.RefinementList.Add((RefinementItem)((System.ICloneable)x).Clone()));
                        }
                    }
                }

                #endregion

                #region Process Hierarchy Dimensions

                var isMasterResults = (query.NonParsedFieldList["masterresults"] == "true" || query.NonParsedFieldList["masterresults"] == "1");
                if (!isMasterResults)
                {
                    var deleteList = new List<DimensionItem>();
                    do
                    {
                        deleteList.Clear();
                        foreach (var dItem in configuration.retval.DimensionList.Where(x => x.Parent != null).ToList())
                        {
                            var parent = configuration.retval.DimensionList.FirstOrDefault(x => x.Name == dItem.Parent.Name);
                            if (parent != null && parent.RefinementList.Count > 1)
                                deleteList.Add(dItem);
                        }
                        deleteList.ForEach(x => configuration.retval.DimensionList.Remove(x));
                    } while (deleteList.Count > 0);
                }

                #endregion

                configuration.retval.Fieldset = schema.FieldList.ToArray();

                //Get a list of applied filters
                foreach (var dvidx in query.DimensionValueList)
                {
                    var dimension = dimensionList.FirstOrDefault(x => x.RefinementList.Any(z => z.DVIdx == dvidx));
                    if (dimension != null)
                    {
                        dimension = ((ICloneable)dimension).Clone() as DimensionItem;
                        if (dimension == null)
                            throw new DatastoreException("The dimension is null");

                        //Remove all refinements that are not this specific filter
                        dimension.RefinementList.RemoveAll(x => x.DVIdx != dvidx);
                        //If there is an existing dimension add refinements to it.
                        //This only applies if there are multiple refinements for a single dimension
                        var existing = configuration.retval.AppliedDimensionList.FirstOrDefault(x => x.DIdx == dimension.DIdx);

                        //Now add the counts to the applied refinements
                        foreach (var ritem in dimension.RefinementList)
                        {
                            ritem.Count = configuration.retval.DimensionList
                                .Where(x => x.DIdx == ritem.DIdx)
                                .SelectMany(x => x.RefinementList)
                                .Where(x => x.DVIdx == ritem.DVIdx)
                                .Select(x => x.Count)
                                .FirstOrDefault();
                        }

                        if (existing != null) existing.RefinementList.AddRange(dimension.RefinementList);
                        else configuration.retval.AppliedDimensionList.Add(dimension);
                    }
                }

                //If this is a custom select list then remove all item array elements except the specified fields
                #region UsingCustomSelect
                if (configuration.usingCustomSelect == ObjectConfiguration.SelectionMode.Custom)
                {
                    foreach (var record in configuration.retval.RecordList)
                    {
                        var newArr = new List<object>();
                        var index = 0;
                        foreach (var field in schema.FieldList)
                        {
                            if (configuration.schema.FieldList.Any(x => x == field))
                                newArr.Add(record.ItemArray[index]);
                            index++;
                        }
                        record.ItemArray = newArr.ToArray();
                    }
                    var fieldDefs = new List<FieldDefinition>();
                    foreach (var field in schema.FieldList)
                    {
                        if (configuration.schema.FieldList.Any(x => x == field))
                            fieldDefs.Add(field);
                    }
                    configuration.retval.Fieldset = fieldDefs.ToArray();
                }
                #endregion

                #region UsingGroupingSelect
                if (configuration.usingCustomSelect == ObjectConfiguration.SelectionMode.Grouping)
                {
                    foreach (var record in configuration.retval.RecordList)
                    {
                        //TODO
                    }
                }
                #endregion

                //Remove all refinements in the applied dimension list
                configuration.retval.DimensionList.ForEach(dimension =>
                {
                    dimension.RefinementList.RemoveAll(x => query.DimensionValueList.Contains(x.DVIdx));
                });

                if (!query.IncludeEmptyDimensions)
                    configuration.retval.DimensionList.RemoveAll(x => x.RefinementList.Count == 0);

                timer.Stop();
                configuration.retval.ComputeTime = timer.ElapsedMilliseconds;

                executeHistory = string.Empty;
#if DEBUG
                executeHistory += (configuration.PerfLoadRecords ? "1" : "0") + ".";
                executeHistory += (configuration.PerfLoadNDim ? "1" : "0") + ".";
                executeHistory += configuration.PerfLoadLDim + ".";
                executeHistory += (configuration.PerfLoadCount ? "1" : "0") + ".";
                executeHistory += (configuration.PerfLoadAgg ? "1" : "0");
#endif
                if (query.IncludeRecords)
                    DataManager.Sync(configuration.retval.RecordList, schema);
                return configuration.retval;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static string GetInnerJoinClause(RepositorySchema schema, RepositorySchema parentSchema, DataQuery query, List<DimensionItem> dimensionList, List<SqlParameter> parameters)
        {
            try
            {
                var retval = new StringBuilder();
                if (query.DimensionValueList != null && query.DimensionValueList.Any())
                {
                    var listDimensions = new List<DimensionItem>();
                    var prepList = new Dictionary<DimensionItem, List<long>>();
                    foreach (var dvidx in query.DimensionValueList.Distinct())
                    {
                        var dItem = dimensionList.GetDimensionByDVIdx(dvidx);
                        if (dItem != null && !listDimensions.Contains(dItem) && schema.DimensionList.First(x => x.DIdx == dItem.DIdx).DataType == RepositorySchema.DataTypeConstants.List)
                            listDimensions.Add(dItem);
                    }

                    foreach (var dItem in listDimensions)
                    {
                        var listTable = GetListTableName(schema.ID, dItem.DIdx);
                        if (parentSchema != null && parentSchema.DimensionList.Any(x => x.DIdx == dItem.DIdx))
                            listTable = GetListTableName(schema.ParentID.Value, dItem.DIdx);

                        retval.Append($" INNER JOIN [{listTable}] {NoLockText()} ON [Z].[{RecordIdxField}] = [{listTable}].[{RecordIdxField}] ");
                    }
                }

                #region Users
                if (schema.UserPermissionField != null && query.UserList != null && query.UserList.Count > 0)
                {
                    var f = schema.FieldList.FirstOrDefault(x => x == schema.UserPermissionField);
                    if (f != null)
                    {
                        var userPermissionTableName = GetUserPermissionTableName(schema.ID);
                        retval.Append($" INNER JOIN [{userPermissionTableName}] [V] {NoLockText()} ON [V].[FKField] = [Z].[{f.TokenName}]");
                    }
                }
                #endregion

                return retval.ToString();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        private static string GetWhereClause(RepositorySchema schema, RepositorySchema parentSchema, DataQuery query, List<DimensionItem> dimensionList, List<SqlParameter> parameters, bool useDimensionOnly = false)
        {
            if (parameters == null)
                parameters = new List<SqlParameter>();

            try
            {
                #region Where

                #region Dimensions

                var retval = new StringBuilder();
                if (query.DimensionValueList != null && query.DimensionValueList.Any())
                {
                    var prepList = new Dictionary<DimensionItem, List<long>>();
                    foreach (var dvidx in query.DimensionValueList.Distinct())
                    {
                        var dItem = dimensionList.GetDimensionByDVIdx(dvidx);
                        if (dItem != null)
                        {
                            if (!prepList.ContainsKey(dItem)) prepList.Add(dItem, new List<long>());
                            prepList[dItem].Add(dvidx);
                        }
                    }

                    foreach (var dItem in prepList.Keys)
                    {
                        var rindex = 0;
                        var dDef = schema.DimensionList.FirstOrDefault(x => x.DIdx == dItem.DIdx);
                        if (dDef != null)
                        {
                            retval.Append("(");
                            if (dDef.DataType == RepositorySchema.DataTypeConstants.List)
                            {
                                #region List
                                var subParameters = new List<SqlParameter>();
                                foreach (var dvidx in prepList[dItem])
                                {
                                    var newParam = new SqlParameter
                                    {
                                        DbType = DbType.Int64,
                                        ParameterName = "@__d_" + dItem.DIdx + "_" + rindex,
                                        Value = dvidx
                                    };
                                    parameters.Add(newParam);
                                    subParameters.Add(newParam);
                                    rindex++;
                                }

                                var listTable = GetListTableName(schema.ID, dDef.DIdx);
                                if (parentSchema != null && parentSchema.DimensionList.Any(x => x.DIdx == dItem.DIdx))
                                    listTable = GetListTableName(schema.ParentID.Value, dItem.DIdx);

                                if (subParameters.Count == 1)
                                {
                                    //This has about half the query cost if can use it vs the "IN" clause
                                    retval.Append($"[{listTable}].[DVIdx] = {subParameters.Select(x => x.ParameterName).First()}");
                                }
                                else if (subParameters.Any())
                                {
                                    retval.Append($"[{listTable}].[DVIdx] IN ({subParameters.Select(x => x.ParameterName).ToCommaList()})");
                                }

                                #endregion
                            }
                            else
                            {
                                #region Non-List
                                foreach (var dvidx in prepList[dItem])
                                {
                                    var newParam = new SqlParameter
                                    {
                                        DbType = DbType.Int64,
                                        ParameterName = "@__d_" + dItem.DIdx + "_" + rindex,
                                        Value = dvidx
                                    };
                                    parameters.Add(newParam);

                                    retval.Append("[Z].[__d" + Utilities.DbTokenize(dItem.Name) + "] = " + newParam.ParameterName);
                                    if (rindex < prepList[dItem].Count - 1)
                                        retval.Append(" OR ");
                                    rindex++;
                                }
                                #endregion
                            }
                            retval.Append(") AND ");
                        }
                        else
                        {
                            LoggerCQ.LogWarning("GetWhereClause could find dimension " + dItem.Name + "|" + dItem.DIdx);
                        }
                    }
                }

                #endregion

                #region Users
                if (schema.UserPermissionField != null && query.UserList != null && query.UserList.Count > 0)
                {
                    var f = schema.FieldList.FirstOrDefault(x => x == schema.UserPermissionField);
                    if (f != null)
                    {
                        if (query.UserList.Count == 1)
                        {
                            //Single user - just set equal
                            retval.Append("[V].[UserId] = @__ul_user AND ");
                            parameters.Add(new SqlParameter
                            {
                                DbType = DbType.Int32,
                                ParameterName = "@__ul_user",
                                Value = query.UserList[0],
                            });
                        }
                        else
                        {
                            //Multi-user - build 'IN' clause
                            var index = 0;
                            var userParamArr = new List<string>();
                            query.UserList.ForEach(x =>
                            {
                                userParamArr.Add($"@__ul_user{index}");
                                index++;
                            });
                            retval.Append($"[V].[UserId] in ({userParamArr.ToCommaList()}) AND ");
                            index = 0;
                            foreach (var userId in query.UserList)
                            {
                                parameters.Add(new SqlParameter
                                {
                                    DbType = DbType.Int32,
                                    ParameterName = $"@__ul_user{index}",
                                    Value = userId,
                                });
                                index++;
                            }
                        }
                    }
                }
                #endregion

                if (!useDimensionOnly)
                {
                    #region Filters

                    if (query.FieldFilters != null)
                    {
                        //Verify there is only one Geo filter
                        if (query.FieldFilters.Count(x => x is GeoCodeFieldFilter) > 1)
                        {
                            throw new Exception("Multiple geo location filters cannot be specified!");
                        }

                        query.FieldFilters = query.FieldFilters.Where(x => !string.IsNullOrEmpty(x.Name)).ToList();

                        //Break into groups
                        var gNames = query.FieldFilters.Select(x => x.Name).Distinct().ToList();
                        foreach (var groupName in gNames)
                        {
                            //this will be used to give unique names to parameters just in case there is a duplicate filter
                            //this way FieldX will have multiple parameters like FieldX1, FieldX2, etc.
                            var parameterIndex = 0;
                            var groupSql = new List<string>();
                            foreach (var filter in query.FieldFilters.Where(x => x.Name == groupName))
                            {
                                var sb = new StringBuilder();
                                GetWhereClauseSingleFilter(filter, schema, sb, parameters, parameterIndex, dimensionList);
                                groupSql.Add(sb.ToString());
                                parameterIndex++;
                            }

                            //Popele use < and > to define searches like (startdate<date && date<enddate)
                            //We need these terms to be AND'ed together
                            //All items in a group are OR'ed togeher
                            retval.Append($"({groupSql.ToStringList(" AND ")})");
                            

                            retval.Append(" AND "); //groups are AND'ed together
                        }

                        foreach (var param in parameters.Where(x => x.Value == null))
                            param.Value = System.DBNull.Value;
                    }

                    #endregion

                    #region Keyword

                    var keyword = (query.Keyword + string.Empty).Replace("\t", " ").Replace("\r\n", " ").Replace("\n\r", " ").Replace("\r", " ").Replace("\n", " ").Trim();
                    if (!string.IsNullOrEmpty(keyword))
                    {
                        //remove quotes
                        var keywordList = System.Text.RegularExpressions.Regex.Matches(keyword, @"[\""].+?[\""]|[^ ]+")
                            .Cast<System.Text.RegularExpressions.Match>()
                            .Select(m => m.Value)
                            .ToList();

                        for (var ii = 0; ii < keywordList.Count; ii++)
                        {
                            if (keywordList[ii].Length > 2 && keywordList[ii].StartsWith("\"") && keywordList[ii].EndsWith("\""))
                                keywordList[ii] = "\"" + keywordList[ii].Substring(1, keywordList[ii].Length - 2).Replace("\"", string.Empty).Replace("'", string.Empty) + "\"";
                            else
                                keywordList[ii] = "\"" + keywordList[ii].Replace("\"", string.Empty).Replace("'", string.Empty) + "\"";
                        }

                        var searchSqlText = GetFTSColumns(schema, true, true);
                        if (!string.IsNullOrEmpty(searchSqlText))
                        {
                            retval.AppendLine("CONTAINS((" + searchSqlText + "), @__fts" + ") AND ");
                            parameters.Add(new SqlParameter { DbType = DbType.String, IsNullable = false, ParameterName = "@__fts", Value = keywordList.ToStringList(" OR ") });
                        }
                    }

                    #endregion
                }

                retval.Append(EmptyWhereClause);

                #endregion

                //Remove unnecessary clause if need be
                return retval.ToString().Replace("AND " + EmptyWhereClause, string.Empty);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        private static void GetWhereClauseSingleFilter(IFieldFilter filter, RepositorySchema schema, StringBuilder sb, List<SqlParameter> parameters, int parameterIndex, List<DimensionItem> dimensionList)
        {
            var filterFound = false;

            sb.Append("(");
            try
            {
                #region Setup
                var ff = ((ICloneable)filter).Clone() as IFieldFilter;
                if (ff == null) throw new Exception("Object cannot be null!");
                var field = schema.FieldList.FirstOrDefault(x => x.Name.Match(ff.Name));
                if (field == null)
                {
                    if (ff.Name == FieldFilterTimestamp.FilterName)
                    {
                        field = new FieldDefinition { DataType = RepositorySchema.DataTypeConstants.Int, Name = FieldFilterTimestamp.FilterName };
                    }
                    else if (ff.Name.Match(HashField))
                    {
                        field = new FieldDefinition { DataType = RepositorySchema.DataTypeConstants.Int, Name = HashField };
                    }
                    else
                    {
                        LoggerCQ.LogWarning("GetWhereClause could not find filter '" + ff.Name + "'");
                        //throw new Exception("Field not found '" + ff.Name + "'!");
                        return;
                    }
                }
                SqlParameter filterParam = null;
                #endregion

                #region GeoCode
                if (field.DataType == RepositorySchema.DataTypeConstants.GeoCode)
                {
                    var geo = (GeoCodeFieldFilter)ff;
                    if (geo != null)
                    {
                        filterFound = true;
                        switch (ff.Comparer)
                        {
                            case ComparisonConstants.LessThan:
                            case ComparisonConstants.LessThanOrEq:
                                sb.Append($"[Z].[{field.TokenName}].STDistance(geography::Point({geo.Latitude}, {geo.Longitude}, 4326)) <= " + (geo.Radius * 1609.344));
                                break;
                            case ComparisonConstants.GreaterThan:
                            case ComparisonConstants.GreaterThanOrEq:
                                sb.Append($"[Z].[{field.TokenName}].STDistance(geography::Point({geo.Latitude}, {geo.Longitude}, 4326)) >= " + (geo.Radius * 1609.344));
                                break;
                            case ComparisonConstants.Equals:
                                //+-25 METERS
                                sb.Append(((geo.Radius * 1609.344) - 25) + $" <= [Z].[{field.TokenName}].STDistance(geography::Point({geo.Latitude}, {geo.Longitude}, 4326)) AND [Z].[{field.TokenName}].STDistance(geography::Point({geo.Latitude}, {geo.Longitude}, 4326)) <= " + ((geo.Radius * 1609.344) + 25));
                                break;
                            case ComparisonConstants.NotEqual:
                                //+-25 METERS
                                sb.Append("(" + ((geo.Radius * 1609.344) - 25) + $" > [Z].[{field.TokenName}].STDistance(geography::Point({geo.Latitude}, {geo.Longitude}, 4326)) OR [Z].[{field.TokenName}].STDistance(geography::Point({geo.Latitude}, {geo.Longitude}, 4326)) > " + ((geo.Radius * 1609.344) + 25) + ")");
                                break;
                            default:
                                throw new Exception("This operation is not supported!");
                        }
                    }
                }
                #endregion

                #region Bool
                else if (field.DataType == RepositorySchema.DataTypeConstants.Bool)
                {
                    var input = GetValueBool(ff.Value);
                    if (ff.Value != null && input == null) return;
                    filterFound = true;
                    switch (ff.Comparer)
                    {
                        case ComparisonConstants.Equals:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.Boolean,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            if (ff.Value == null)
                                sb.Append($"[Z].[{field.TokenName}] IS {filterParam.ParameterName}");
                            else
                                sb.Append($"[Z].[{field.TokenName}] = {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.NotEqual:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.Boolean,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            if (ff.Value == null)
                                sb.Append($"[Z].[{field.TokenName}] IS NOT {filterParam.ParameterName}");
                            else
                                sb.Append($"[Z].[{field.TokenName}] <> {filterParam.ParameterName}");
                            break;
                        default:
                            throw new Exception("This operation is not supported!");
                    }
                }
                #endregion

                #region DateTime
                else if (field.DataType == RepositorySchema.DataTypeConstants.DateTime)
                {
                    var input = GetValueDateTime(ff.Value);
                    if (ff.Value != null && input == null) return;
                    filterFound = true;
                    switch (ff.Comparer)
                    {
                        case ComparisonConstants.LessThan:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.DateTime,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] < {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.LessThanOrEq:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.DateTime,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] <= {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.GreaterThan:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.DateTime,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] > {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.GreaterThanOrEq:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.DateTime,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] >= {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.Equals:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.DateTime,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            if (ff.Value == null)
                                sb.Append($"[Z].[{field.TokenName}] IS NULL");
                            else
                                sb.Append($"[Z].[{field.TokenName}] = {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.NotEqual:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.DateTime,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            if (ff.Value == null)
                                sb.Append($"[Z].[{field.TokenName}] IS NOT NULL");
                            else
                                sb.Append($"[Z].[{field.TokenName}] <> {filterParam.ParameterName}");
                            break;
                        default:
                            throw new Exception("This operation is not supported!");
                    }
                }
                #endregion

                #region Float
                else if (field.DataType == RepositorySchema.DataTypeConstants.Float)
                {
                    var input = GetValueDouble(ff.Value);
                    if (ff.Value != null && input == null) return;
                    filterFound = true;
                    switch (ff.Comparer)
                    {
                        case ComparisonConstants.LessThan:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.Double,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] < {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.LessThanOrEq:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.Double,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] <= {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.GreaterThan:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.Double,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] > {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.GreaterThanOrEq:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.Double,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] >= {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.Equals:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.Double,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            if (ff.Value == null)
                                sb.Append($"[Z].[{field.TokenName}] IS NULL");
                            else
                                sb.Append($"[Z].[{field.TokenName}] = {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.NotEqual:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.Double,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            if (ff.Value == null)
                                sb.Append($"[Z].[{field.TokenName}] IS NOT NULL");
                            else
                                sb.Append($"[Z].[{field.TokenName}] <> {filterParam.ParameterName}");
                            break;
                        default:
                            throw new Exception("This operation is not supported!");
                    }
                }
                #endregion

                #region Int
                else if (field.DataType == RepositorySchema.DataTypeConstants.Int)
                {
                    var input = GetValueInt(ff.Value);
                    if (ff.Value != null && input == null) return;
                    filterFound = true;
                    var dataType = DbType.Int32;

                    //This is sort of a hack but if it is the Hash field then strong type to Int64
                    if (ff.Name == HashField) dataType = DbType.Int64;

                    switch (ff.Comparer)
                    {
                        case ComparisonConstants.LessThan:
                            filterParam = new SqlParameter
                            {
                                DbType = dataType,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] < {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.LessThanOrEq:
                            filterParam = new SqlParameter
                            {
                                DbType = dataType,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] => " + filterParam.ParameterName);
                            break;
                        case ComparisonConstants.GreaterThan:
                            filterParam = new SqlParameter
                            {
                                DbType = dataType,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] > {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.GreaterThanOrEq:
                            filterParam = new SqlParameter
                            {
                                DbType = dataType,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] >= {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.Equals:
                            filterParam = new SqlParameter
                            {
                                DbType = dataType,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            if (ff.Value == null)
                                sb.Append($"[Z].[{field.TokenName}] IS NULL");
                            else
                                sb.Append($"[Z].[{field.TokenName}] = {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.NotEqual:
                            filterParam = new SqlParameter
                            {
                                DbType = dataType,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            if (ff.Value == null)
                                sb.Append($"[Z].[{field.TokenName}] IS NOT NULL");
                            else
                                sb.Append($"[Z].[{field.TokenName}] <> {filterParam.ParameterName}");
                            break;
                        default:
                            throw new Exception("This operation is not supported!");
                    }
                }
                #endregion

                #region Int64
                else if (field.DataType == RepositorySchema.DataTypeConstants.Int64)
                {
                    var input = GetValueInt64(ff.Value);
                    if (ff.Value != null && input == null) return;
                    filterFound = true;
                    var dataType = DbType.Int64;

                    //This is sort of a hack but if it is the Hash field then strong type to Int64
                    if (ff.Name == HashField) dataType = DbType.Int64;

                    switch (ff.Comparer)
                    {
                        case ComparisonConstants.LessThan:
                            filterParam = new SqlParameter
                            {
                                DbType = dataType,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] < {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.LessThanOrEq:
                            filterParam = new SqlParameter
                            {
                                DbType = dataType,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] => " + filterParam.ParameterName);
                            break;
                        case ComparisonConstants.GreaterThan:
                            filterParam = new SqlParameter
                            {
                                DbType = dataType,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] > {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.GreaterThanOrEq:
                            filterParam = new SqlParameter
                            {
                                DbType = dataType,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] >= {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.Equals:
                            filterParam = new SqlParameter
                            {
                                DbType = dataType,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            if (ff.Value == null)
                                sb.Append($"[Z].[{field.TokenName}] IS NULL");
                            else
                                sb.Append($"[Z].[{field.TokenName}] = {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.NotEqual:
                            filterParam = new SqlParameter
                            {
                                DbType = dataType,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = input,
                            };
                            parameters.Add(filterParam);
                            if (ff.Value == null)
                                sb.Append($"[Z].[{field.TokenName}] IS NOT NULL");
                            else
                                sb.Append($"[Z].[{field.TokenName}] <> {filterParam.ParameterName}");
                            break;
                        default:
                            throw new Exception("This operation is not supported!");
                    }
                }
                #endregion

                #region String
                else if (field.DataType == RepositorySchema.DataTypeConstants.String)
                {
                    string ffValue = null;
                    if (ff.Value is string) ffValue = (string)ff.Value;
                    else if (ff.Value != null) ffValue = ff.Value.ToString();
                    if (ffValue != NULLVALUE) ff.Value = null;
                    filterFound = true;
                    switch (ff.Comparer)
                    {
                        case ComparisonConstants.LessThan:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.String,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = ffValue
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] < {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.LessThanOrEq:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.String,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = ffValue
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] <= {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.GreaterThan:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.String,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = ffValue
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] > {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.GreaterThanOrEq:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.String,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = ffValue
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] >= {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.Equals:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.String,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = ffValue
                            };
                            parameters.Add(filterParam);
                            if (ffValue == null)
                                sb.Append($"[Z].[{field.TokenName}] IS NULL");
                            else
                                sb.Append($"[Z].[{field.TokenName}] = {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.NotEqual:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.String,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = ffValue
                            };
                            parameters.Add(filterParam);
                            if (ffValue == null)
                                sb.Append($"[Z].[{field.TokenName}] IS NOT NULL");
                            else
                                sb.Append($"[Z].[{field.TokenName}] <> {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.Like:
                            filterParam = new SqlParameter
                            {
                                DbType = DbType.String,
                                ParameterName = field.ToParameterName(parameterIndex),
                                Value = ffValue?.Replace("*", "%"),
                            };
                            parameters.Add(filterParam);
                            sb.Append($"[Z].[{field.TokenName}] LIKE {filterParam.ParameterName}");
                            break;
                        case ComparisonConstants.ContainsAny:
                            if (ffValue != null)
                            {
                                var words = ffValue.Replace("*", string.Empty).Replace("%", string.Empty).Split(new char[] { '^' }, StringSplitOptions.RemoveEmptyEntries);
                                if (words.Any())
                                {
                                    var subSqlList = new List<string>();
                                    var subIndex = 0;
                                    foreach (var word in words)
                                    {
                                        filterParam = new SqlParameter
                                        {
                                            DbType = DbType.String,
                                            ParameterName = field.ToParameterName(parameterIndex, subIndex),
                                            Value = $"%{word}%",
                                        };
                                        parameters.Add(filterParam);
                                        subSqlList.Add($"[Z].[{field.TokenName}] LIKE {filterParam.ParameterName}");
                                        subIndex++;
                                    }
                                    sb.Append($"({subSqlList.ToStringList(" OR ")})");
                                }
                            }
                            break;
                        case ComparisonConstants.ContainsAll:
                            if (ffValue != null)
                            {
                                var words = ffValue.Replace("*", string.Empty).Replace("%", string.Empty).Split(new char[] { '^' }, StringSplitOptions.RemoveEmptyEntries);
                                if (words.Any())
                                {
                                    var subSqlList = new List<string>();
                                    var subIndex = 0;
                                    foreach (var word in words)
                                    {
                                        filterParam = new SqlParameter
                                        {
                                            DbType = DbType.String,
                                            ParameterName = field.ToParameterName(parameterIndex, subIndex),
                                            Value = $"%{word}%",
                                        };
                                        parameters.Add(filterParam);
                                        subSqlList.Add($"[Z].[{field.TokenName}] LIKE {filterParam.ParameterName}");
                                        subIndex++;
                                    }
                                    sb.Append($"({subSqlList.ToStringList(" AND ")})");
                                }
                            }
                            break;
                        case ComparisonConstants.ContainsNone:
                            if (ffValue != null)
                            {
                                var words = ffValue.Replace("*", string.Empty).Replace("%", string.Empty).Split(new char[] { '^' }, StringSplitOptions.RemoveEmptyEntries);
                                if (words.Any())
                                {
                                    var subSqlList = new List<string>();
                                    var subIndex = 0;
                                    foreach (var word in words)
                                    {
                                        filterParam = new SqlParameter
                                        {
                                            DbType = DbType.String,
                                            ParameterName = field.ToParameterName(parameterIndex, subIndex),
                                            Value = $"%{word}%",
                                        };
                                        parameters.Add(filterParam);
                                        subSqlList.Add($"([Z].[{field.TokenName}] NOT LIKE {filterParam.ParameterName})");
                                        subIndex++;
                                    }
                                    sb.Append($"({subSqlList.ToStringList(" AND ")})");
                                }
                            }
                            break;
                        default:
                            throw new Exception("This operation is not supported!");
                    }
                }
                #endregion

                #region List
                else if (field.DataType == RepositorySchema.DataTypeConstants.List)
                {
                    var dimension = dimensionList.FirstOrDefault(x => x.Name == filter.Name);
                    List<IRefinementItem> rList = null;
                    var theValue = string.Empty;
                    if (filter.Value is string[])
                    {
                        var v1 = filter.Value as string[];
                        if (v1 != null) theValue = v1.FirstOrDefault();
                        if (theValue == NULLVALUE)
                        {
                            //If we are testing NULL then compare against all refinements and set the value to REAL NULL
                            theValue = null;
                            rList = dimension.RefinementList.ToList();
                        }
                        else
                        {
                            rList = dimension.RefinementList.Where(x => !string.IsNullOrEmpty(x.FieldValue) && x.FieldValue.IndexOf(theValue, StringComparison.OrdinalIgnoreCase) >= 0).ToList();
                            rList = dimension.RefinementList.Where(x => !string.IsNullOrEmpty(x.FieldValue) && v1.Contains(x.FieldValue)).ToList();
                        }
                    }
                    else
                    {
                        var v1 = filter.Value as string;
                        if (v1 != null) theValue = v1;
                        if (theValue == NULLVALUE)
                        {
                            //If we are testing NULL then compare against all refinements and set the value to REAL NULL
                            theValue = null;
                            rList = dimension.RefinementList.ToList();
                        }
                        else
                        {
                            rList = dimension.RefinementList.Where(x => !string.IsNullOrEmpty(x.FieldValue) && x.FieldValue.IndexOf(theValue, StringComparison.OrdinalIgnoreCase) >= 0).ToList();
                        }

                    }

                    if (dimension != null)
                    {
                        var listTable = GetListTableName(schema.ID, dimension.DIdx);
                        filterFound = true;
                        switch (ff.Comparer)
                        {
                            case ComparisonConstants.Equals:
                                if (!string.IsNullOrEmpty(theValue) && rList.Count == 0) sb.Append("1=0"); //if match something and no items then null set, no matches
                                else if (theValue == null) sb.Append($"Z.{RecordIdxField} NOT in (select {RecordIdxField} from [{listTable}])");
                                else if (theValue != null && rList.Count == 0) sb.Append("1=0"); //if looking for something and no items then nothing matches
                                else if (theValue != null && rList.Count != 0) sb.Append($"Z.{RecordIdxField} in (select {RecordIdxField} from [{listTable}] where [{listTable}].DVIdx in ({rList.Select(z => z.DVIdx).ToCommaList()}))");
                                break;
                            case ComparisonConstants.NotEqual:
                                if (string.IsNullOrEmpty(theValue) && rList.Count == 0) sb.Append(EmptyWhereClause); //if looking for NOT NULL and no items then everything matches
                                else if (theValue == null) sb.Append($"Z.{RecordIdxField} in (select {RecordIdxField} from [{listTable}])"); //looking for NOT NULL so match anything that exists
                                else if (theValue != null && rList.Count == 0) sb.Append(EmptyWhereClause); //if looking for NOT something and no items then everything matches
                                else if (theValue != null && rList.Count != 0) sb.Append($"Z.{RecordIdxField} NOT in (select {RecordIdxField} from [{listTable}] where [{listTable}].DVIdx in ({rList.Select(z => z.DVIdx).ToCommaList()}))"); //looking for NOT some value, so match all rows NOT matching value
                                break;
                            case ComparisonConstants.Like:
                                if (string.IsNullOrEmpty(theValue)) sb.Append("1=0");
                                else
                                {
                                    filterParam = new SqlParameter
                                    {
                                        DbType = DbType.String,
                                        ParameterName = $"@__ff{Utilities.CodeTokenize(field.Name)}LC{parameterIndex}",
                                        Value = theValue.Replace("*", "%"),
                                    };
                                    parameters.Add(filterParam);
                                    var dimensionValueTable = GetDimensionValueTableName(schema.ID);
                                    sb.Append($"Z.{RecordIdxField} in (select {RecordIdxField} from [{listTable}] inner join [{dimensionValueTable}] [DV_{ff.Name}] on [{listTable}].[DVIdx] = [DV_{ff.Name}].[DVIdx] where (");
                                    sb.Append($"[DV_{ff.Name}].[Value] LIKE {filterParam.ParameterName}");
                                    sb.Append("))");
                                }
                                break;
                            default:
                                throw new Exception("List dimensions only support the operators: Equals, NotEquals, Like.");
                        }
                    }
                    else //field
                    {
                        //TODO
                    }
                }
                #endregion

                #region Log warning for no index if need be
                if (!field.AllowIndex &&
                    field.DataType != RepositorySchema.DataTypeConstants.GeoCode &&
                    field.DataType != RepositorySchema.DataTypeConstants.List &&
                    field.Name != HashField &&
                    field.Name != TimestampField)
                    LoggerCQ.LogTrace($"WhereField NoIndex: ID={schema.ID}, Field={field.Name}");
                #endregion

            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                if (!filterFound) sb.Append(EmptyWhereClause);
                sb.Append(")");
            }
        }

        public static string QueryAsync(RepositorySchema schema, int repositoryId, DataQuery query, List<DimensionItem> dimensionList, List<SqlParameter> parameters, string connectionString)
        {
            var dataTable = GetTableName(schema);
            if (schema.ParentID != null)
                dataTable = GetTableViewName(schema.ID);

            RepositorySchema parentSchema = null;
            if (schema.ParentID != null)
            {
                parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);
            }

            var orderByClause = $"[Z].[{RecordIdxField}] ASC";
            if (parameters == null) parameters = new List<SqlParameter>();

            var whereClause = GetWhereClause(schema, parentSchema, query, dimensionList, parameters);
            var innerJoinClause = GetInnerJoinClause(schema, parentSchema, query, dimensionList, parameters);
            var normalFields = schema.FieldList.Where(x => x.DataType != RepositorySchema.DataTypeConstants.List).ToList();

            #region Order By

            if (query.FieldSorts != null && query.FieldSorts.Any())
            {
                var usedFields = new List<string>();
                orderByClause = string.Empty;
                foreach (var sf in query.FieldSorts)
                {
                    //Do not allow duplicates
                    if (!usedFields.Any(x => x == sf.Name))
                    {
                        if (schema.FieldList.Any(x => x.DataType == RepositorySchema.DataTypeConstants.List && x.Name == sf.Name))
                        {
                            //throw new Exception("Invalid sort by '" + sf.Name + "' column");
                        }
                        else if (schema.FieldList.Any(x => x.Name.Match(sf.Name))) //Ensure this is a valid field
                        {
                            orderByClause += $"[Z].[{sf.TokenName}] {sf.ToSqlDirection()}, ";
                        }
                        usedFields.Add(sf.Name);
                    }
                }
                orderByClause += $"[Z].[{RecordIdxField}] ASC";
            }

            #endregion

            #region Build/Execute SQL

            var fieldListSql = new List<string>();
            foreach (var field in normalFields)
            {
                fieldListSql.Add($"[Z].[{field.TokenName}]");
            }
            var fieldSql = $"{fieldListSql.ToCommaList()}, [Z].[{RecordIdxField}], [Z].[{TimestampField}]";

            var sbSql = new StringBuilder();

            #region Records
            //If supports OFFSET/FETCH then use it
            //No paging...this is faster
            sbSql.AppendLine($"SELECT {fieldSql}");
            sbSql.AppendLine($"FROM [{dataTable}] Z {NoLockText()} {innerJoinClause}");
            sbSql.AppendLine($"WHERE {whereClause}");
            sbSql.AppendLine($"ORDER BY {orderByClause}");
            sbSql.AppendLine();

            parameters.Add(new SqlParameter { DbType = DbType.Int32, ParameterName = "@repositoryid", Value = repositoryId });

            #endregion

            #endregion

            var sql = sbSql.ToString();
            sql = sql.Replace($" AND {EmptyWhereClause}", string.Empty);
            sql = sql.Replace($" WHERE {EmptyWhereClause}", string.Empty);
            return sql;
        }

        #endregion

        #region GetLastTimestamp

        public static int GetLastTimestamp(RepositorySchema schema, int repositoryId, DataQuery query, List<DimensionItem> dimensionList)
        {
            query.FieldSorts.Clear();
            query.FieldSorts.Add(new FieldSortTimestamp() { SortDirection = SortDirectionConstants.Desc });
            query.RecordsPerPage = 1;
            query.PageOffset = 1;
            var results = Query(schema, repositoryId, query, dimensionList);
            if (results.RecordList.Any())
                return results.RecordList.First().__Timestamp;
            else
                return 0;
        }

        #endregion

        #region Count

        public static int Count(RepositorySchema schema, int repositoryId, string connectionString)
        {
            try
            {
                var dataTable = GetTableName(schema);
                var ds = GetDataset(connectionString, $"select count(*) from [{dataTable}]");
                if (ds.Tables.Count == 1 && ds.Tables[0].Rows.Count == 1)
                    return (int)ds.Tables[0].Rows[0][0];
                return 0;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        #endregion

        #region Clear

        public static void Clear(RepositorySchema schema, string connectionString)
        {
            try
            {
                RepositorySchema parentSchema = null;
                var hasChildren = false;
                if (schema.ParentID != null)
                    parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);
                else
                    hasChildren = RepositoryManager.GetChildRepositories(schema.ID).Any();

                var dataTable = GetTableName(schema);
                var sb = new StringBuilder();

                //If there is no binding view then truncate table
                var viewName = GetTableViewName(schema.ID);
                if (!hasChildren)
                {
                    sb.AppendLine($"if not exists(select * from sys.objects where name = '{viewName}' and type = 'V')");
                    sb.AppendLine($"TRUNCATE TABLE [{dataTable}];");
                }

                sb.AppendLine($"UPDATE [Repository] SET [ItemCount] = 0, [Changestamp] = {GetChangeStamp()} WHERE [UniqueKey] = '{schema.ID}';");

                foreach (var d in schema.DimensionList.Where(x => x.DataType == RepositorySchema.DataTypeConstants.List).ToList())
                {
                    var listTable = GetListTableName(schema.ID, d.DIdx);
                    if (parentSchema != null && parentSchema.DimensionList.Any(x => x.DIdx == d.DIdx))
                        listTable = GetListTableName(schema.ParentID.Value, d.DIdx);

                    sb.AppendLine($"TRUNCATE TABLE [{listTable}];");
                }

                var userPermissionTableName = GetUserPermissionTableName(schema.ID);
                sb.AppendLine($"if exists (select * from sys.objects where name='{userPermissionTableName}' and type = 'U')");
                sb.AppendLine($"TRUNCATE TABLE [{userPermissionTableName}];");

                ExecuteSql(connectionString, sb.ToString());

                //Now if there is a binding view we need to delete records
                //Binding view will not allow truncate
                var completeDelete = false;
                do
                {
                    //delete in batches of 10k so locks will not get out of control
                    sb = new StringBuilder();
                    sb.AppendLine($"if exists(select * from sys.objects where name = '{viewName}' and type = 'V')");
                    sb.AppendLine("BEGIN");
                    sb.AppendLine($"SET ROWCOUNT {DeleteBlockSize}");
                    sb.AppendLine($"delete from [{dataTable}];");
                    sb.AppendLine($"select count(*) from [{dataTable}]");
                    sb.AppendLine("END");
                    sb.AppendLine("ELSE");
                    sb.AppendLine("select 0");
                    sb.AppendLine("SET ROWCOUNT 0");
                    var ds = GetDataset(connectionString, sb.ToString());
                    completeDelete = ((int)ds.Tables[0].Rows[0][0] == 0);
                } while (!completeDelete);

                MarkUpdated(schema.ID);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        #endregion

        #region DeleteData

        public static SqlResults DeleteData(RepositorySchema schema, IEnumerable<DataItem> list, string connectionString)
        {
            var retval = new SqlResults();
            try
            {
                var childTables = new List<Guid>();
                if (schema.ParentID == null)
                {
                    using (var context = new DatastoreEntities(connectionString))
                    {
                        childTables = context.Repository.Where(x => x.ParentId == schema.InternalID).Select(x => x.UniqueKey).ToList();
                    }
                }

                var count = 0;
                var dataTable = GetTableName(schema);
                foreach (var item in list)
                {
                    #region Setup parameters
                    var pkIndex = schema.FieldList.IndexOf(schema.PrimaryKey);
                    var parameters = new List<SqlParameter>();
                    SqlParameter newParam = null;
                    switch (schema.PrimaryKey.DataType)
                    {
                        case RepositorySchema.DataTypeConstants.Bool:
                            newParam = new SqlParameter
                            {
                                DbType = DbType.Boolean,
                                IsNullable = true,
                                ParameterName = "@pk",
                                Value = (bool?)item.ItemArray[pkIndex]
                            };
                            break;
                        case RepositorySchema.DataTypeConstants.DateTime:
                            newParam = new SqlParameter
                            {
                                DbType = DbType.DateTime,
                                IsNullable = true,
                                ParameterName = "@pk",
                                Value = (DateTime?)item.ItemArray[pkIndex]
                            };
                            break;
                        case RepositorySchema.DataTypeConstants.Float:
                            newParam = new SqlParameter
                            {
                                DbType = DbType.Double,
                                IsNullable = true,
                                ParameterName = "@pk",
                                Value = (double?)item.ItemArray[pkIndex]
                            };
                            break;
                        case RepositorySchema.DataTypeConstants.GeoCode:
                            //TODO
                            break;
                        case RepositorySchema.DataTypeConstants.Int:
                            newParam = new SqlParameter
                            {
                                DbType = DbType.Int32,
                                IsNullable = true,
                                ParameterName = "@pk",
                                Value = (int?)item.ItemArray[pkIndex]
                            };
                            break;
                        case RepositorySchema.DataTypeConstants.Int64:
                            newParam = new SqlParameter
                            {
                                DbType = DbType.Int64,
                                IsNullable = true,
                                ParameterName = "@pk",
                                Value = (long?)item.ItemArray[pkIndex]
                            };
                            break;
                        case RepositorySchema.DataTypeConstants.String:
                            newParam = new SqlParameter
                            {
                                DbType = DbType.String,
                                IsNullable = true,
                                ParameterName = "@pk",
                                Value = (string)item.ItemArray[pkIndex]
                            };
                            break;
                    }
                    parameters.Add(newParam);
                    #endregion

                    var sb = new StringBuilder();
                    sb.AppendLine($"--MARKER 13");
                    sb.AppendLine($"DECLARE {OldRecordIdxField} INT");
                    if (schema.ParentID == null)
                    {
                        if (childTables.Count > 0)
                        {
                            sb.AppendLine($"SET {OldRecordIdxField} = (SELECT TOP 1 [{RecordIdxField}] FROM [{dataTable}] WITH (UPDLOCK) WHERE [{schema.PrimaryKey.TokenName}] = @pk);");
                        }
                        sb.AppendLine($"DELETE FROM [{dataTable}] WHERE [{schema.PrimaryKey.TokenName}] = @pk");
                    }
                    else
                    {
                        var dataTableBase = GetTableName(schema.ParentID.Value);
                        sb.AppendLine($"SET {OldRecordIdxField} = (SELECT TOP 1 [{RecordIdxField}] FROM [{dataTableBase}] WITH (UPDLOCK) WHERE [{schema.PrimaryKey.TokenName}] = @pk);");
                        sb.AppendLine($"DELETE FROM [{dataTableBase}] WHERE [{schema.PrimaryKey.TokenName}] = @pk");
                        sb.AppendLine($"DELETE FROM [{dataTable}] WHERE [{RecordIdxField}] = {OldRecordIdxField}");
                    }

                    #region Process child tables
                    //If there are child tables then remove the records from them
                    if (childTables.Count > 0)
                    {
                        foreach (var childId in childTables)
                        {
                            sb.AppendLine($"DELETE FROM [{GetTableName(childId)}] WHERE [{RecordIdxField}] = {OldRecordIdxField}");
                        }
                    }
                    #endregion

                    AddRepositoryChangedSql(schema.ID, parameters);
                    count = ExecuteSql(connectionString, sb.ToString(), parameters);
                    MarkUpdated(schema.ID);
                }
                retval.AffectedCount = count;
                return retval;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public static SqlResults DeleteData(RepositorySchema schema, DataQuery query, List<DimensionItem> dimensionList, string connectionString)
        {
            const int MaxTry = 5;
            var tryCount = 0;
            var retval = new SqlResults();
            do
            {
                try
                {
                    var dataTable = GetTableName(schema);
                    if (schema.ParentID != null)
                        dataTable = GetTableViewName(schema.ID);

                    var childTables = new List<Guid>();
                    if (schema.ParentID == null)
                    {
                        using (var context = new DatastoreEntities(connectionString))
                        {
                            childTables = context.Repository
                                .Where(x => x.ParentId == schema.InternalID)
                                .Select(x => x.UniqueKey)
                                .ToList();
                        }
                    }

                    RepositorySchema parentSchema = null;
                    if (schema.ParentID != null)
                        parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);

                    var parameters = new List<SqlParameter>();
                    var whereClause = GetWhereClause(schema, parentSchema, query, dimensionList, parameters);
                    var innerJoinClause = GetInnerJoinClause(schema, parentSchema, query, dimensionList, parameters);

                    var sb = new StringBuilder();
                    sb.AppendLine("set nocount on;");

                    sb.AppendLine("declare @_count int = 0");
                    sb.AppendLine($"if not exists(SELECT TOP 1 1 FROM [{dataTable}] Z {NoLockText()} {innerJoinClause} WHERE {whereClause})");
                    sb.AppendLine("goto TheEndOfTheScript;");

                    #region List Dimensions
                    var listDimensions = schema.FieldList
                        .Where(x => x.DataType == RepositorySchema.DataTypeConstants.List &&
                            x is DimensionDefinition)
                            .Cast<DimensionDefinition>()
                        .ToList();

                    if (listDimensions.Count > 0)
                    {
                        sb.AppendLine($"--MARKER 14");
                        sb.AppendLine($"	select [{RecordIdxField}] into #_t1524 from [{dataTable}] Z {NoLockText()} {innerJoinClause} WHERE {whereClause};");

                        //This is a single select for each list dimension
                        foreach (var dimension in listDimensions)
                        {
                            var listTable = GetListTableName(schema.ID, dimension.DIdx);
                            if (parentSchema != null && parentSchema.DimensionList.Any(x => x.DIdx == dimension.DIdx))
                                listTable = GetListTableName(schema.ParentID.Value, dimension.DIdx);

                            sb.AppendLine($"WITH S([{RecordIdxField}])");
                            sb.AppendLine("AS");
                            sb.AppendLine("(");
                            sb.AppendLine($"	select [{RecordIdxField}] from #_t1524");
                            sb.AppendLine(")");
                            sb.AppendLine($"DELETE FROM [{listTable}]");
                            sb.AppendLine($"FROM S inner join [{listTable}] on S.[{RecordIdxField}] = [{listTable}].[{RecordIdxField}];");
                            sb.AppendLine();
                        }
                    }
                    #endregion
                    sb.AppendLine("set nocount off;");

                    //TODO: Check if repository has children and if so remove related records in derived tables
                    if (schema.ParentID == null)
                    {
                        if (childTables.Count == 0)
                        {
                            sb.AppendLine($"--MARKER 15");
                            sb.AppendLine($"DELETE Z WITH (UPDLOCK) FROM [{dataTable}] Z{innerJoinClause} WHERE {whereClause}");
                        }
                        else
                        {
                            sb.AppendLine($"--MARKER 16");
                            sb.AppendLine($"WITH Z ([{RecordIdxField}])");
                            sb.AppendLine("AS");
                            sb.AppendLine("(");
                            sb.AppendLine($"SELECT [Z].[{RecordIdxField}]");
                            sb.AppendLine($"FROM [{dataTable}] Z{innerJoinClause} WHERE {whereClause}");
                            sb.AppendLine(")");
                            sb.AppendLine($"select [{RecordIdxField}] into #t from Z;");
                            sb.AppendLine("DELETE A");
                            sb.AppendLine($"FROM [{GetTableName(schema.ID)}] A INNER JOIN #t ON A.[{RecordIdxField}] = #t.[{RecordIdxField}];");

                            #region Process child tables
                            ////If there are child tables then remove the records from them
                            //if (childTables.Count > 0)
                            //{
                            //    foreach (var childId in childTables)
                            //    {
                            //        sb.AppendLine("DELETE A");
                            //        sb.AppendLine("FROM [" + GetTableName(childId) + "] A INNER JOIN #t ON A.["+ RecordIdxField + "] = #t.["+ RecordIdxField + "];");
                            //    }
                            //}
                            #endregion
                        }

                        AddRepositoryChangedSql(schema.ID, parameters);
                        sb.AppendLine(";set @_count = @@ROWCOUNT");
                        sb.AppendLine(";TheEndOfTheScript:");
                        sb.AppendLine(";select @_count;");
                        retval.AffectedCount = ExecuteSql(connectionString, sb.ToString(), parameters, true, true);
                    }
                    else
                    {
                        sb.AppendLine($"--MARKER 17");
                        sb.AppendLine($"WITH Z ([{RecordIdxField}])");
                        sb.AppendLine("AS");
                        sb.AppendLine("(");
                        sb.AppendLine($"SELECT [Z].[{RecordIdxField}]");
                        sb.AppendLine($"FROM [{dataTable}] Z{innerJoinClause} WHERE {whereClause}");
                        sb.AppendLine(")");
                        sb.AppendLine($"select [{RecordIdxField}] into #t from Z;");
                        sb.AppendLine("DELETE A");
                        sb.AppendLine($"FROM [{GetTableName(schema.ParentID.Value)}] A INNER JOIN #t ON A.[{RecordIdxField}] = #t.[{RecordIdxField}];");
                        sb.AppendLine("DELETE A");
                        sb.AppendLine($"FROM [{GetTableName(schema.ID)}] A INNER JOIN #t ON A.[{RecordIdxField}] = #t.[{RecordIdxField}];");
                        AddRepositoryChangedSql(schema.ID, parameters);
                        sb.AppendLine(";set @_count = @@ROWCOUNT");
                        sb.AppendLine(";TheEndOfTheScript:");
                        sb.AppendLine(";select @_count;");
                        retval.AffectedCount = ExecuteSql(connectionString, sb.ToString(), parameters, true, true);
                    }

                    MarkUpdated(schema.ID);
                    return retval;
                }
                catch (SqlException ex)
                {
                    if (ex.Message.ToLower().Contains("deadlock"))
                    {
                        tryCount++;
                        LoggerCQ.LogWarning(ex, $"DeleteData deadlock: ID={schema.ID}, ThreadId={System.Threading.Thread.CurrentThread.ManagedThreadId}, Try={tryCount}");
                    }
                    else
                        throw;
                    System.Threading.Thread.Sleep(_rnd.Next(150, 500));
                }
                catch (Exception ex)
                {
                    LoggerCQ.LogError(ex);
                    throw;
                }
            } while (tryCount < MaxTry);
            return retval;
        }

        #endregion

        #region GetTypedDimValue

        public static string GetTypedDimValue(RepositorySchema.DataTypeConstants type, object v)
        {
            try
            {
                var retval = string.Empty;
                switch (type)
                {
                    case RepositorySchema.DataTypeConstants.Bool:
                        retval = (bool)v ? "true" : "false";
                        break;
                    case RepositorySchema.DataTypeConstants.Float:
                        retval = ((double)v).ToString();
                        break;
                    case RepositorySchema.DataTypeConstants.Int:
                        retval = ((int)v).ToString();
                        break;
                    case RepositorySchema.DataTypeConstants.Int64:
                        retval = ((long)v).ToString();
                        break;
                    case RepositorySchema.DataTypeConstants.String:
                        retval = (string)v;
                        break;
                    case RepositorySchema.DataTypeConstants.List:
                        throw new Exception("Dimension type List not supported");
                    case RepositorySchema.DataTypeConstants.DateTime:
                        retval = ((DateTime)v).ToString(DimensionItem.DateTimeFormat);
                        break;
                    default:
                        throw new Exception("Invalid dimension data type.");
                }
                return retval;
            }
            catch (Exception ex)
            {
                var debugText = $"GetTypedDimValue: Type={type.ToString()}, Value={(v == null ? "NULL" : v.ToString())}";
                LoggerCQ.LogError(ex, debugText);
                throw;
            }
        }

        #endregion

        #region GetChangeStamp

        internal static int GetChangeStamp()
        {
            return EncryptionDomain.Hash(DateTime.Now.Ticks.ToString() + _rnd.Next(100, 999999).ToString());
        }

        #endregion

        #region GetFileGroups

        public static List<string> GetFileGroups(string connectionString)
        {
            try
            {
                var sql = $"select name from sys.filegroups where (name <> 'PRIMARY' AND name <> '{SetupConfig.YFileGroup}' AND name <> '{SetupConfig.IndexFileGroup}') and type_desc <> 'MEMORY_OPTIMIZED_DATA_FILEGROUP'";
                var ds = GetDataset(connectionString, sql, new List<SqlParameter>());
                return (from DataRow dr in ds.Tables[0].Rows select (string)dr[0]).ToList();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return new List<string>();
            }
        }

        #endregion

        #region SqlVersion

        public static ConfigHelper.ServerVersionConstants GetSqlVersion(string connectionString)
        {
            try
            {
                //Give small timeout
                var c = new SqlConnectionStringBuilder(connectionString);
                c.ConnectTimeout = 8;
                connectionString = c.ToString();

                var ds = GetDataset(connectionString, "select SERVERPROPERTY ('productversion')", new List<SqlParameter>());
                var v = ((string)ds.Tables[0].Rows[0][0] + string.Empty).Trim().ToLower();

                var major = int.Parse(v.Split('.').First());
                if (major == 12)
                    return ConfigHelper.ServerVersionConstants.SQL2014;
                else if (major == 11)
                    return ConfigHelper.ServerVersionConstants.SQL2012;
                else if (major == 10)
                    return ConfigHelper.ServerVersionConstants.SQL2008;
                else if (major < 10)
                    return ConfigHelper.ServerVersionConstants.SQLInvalid;
                else
                    return ConfigHelper.ServerVersionConstants.SQLOther; //Newer version
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return ConfigHelper.ServerVersionConstants.SQLInvalid;
            }
        }

        #endregion

        #region IsEnterpiseVersion

        public static bool IsEnterpiseVersion(string connectionString)
        {
            try
            {
                //Give small timeout
                var c = new SqlConnectionStringBuilder(connectionString);
                c.ConnectTimeout = 8;
                connectionString = c.ToString();

                var ds = GetDataset(connectionString, "select SERVERPROPERTY ('edition')", new List<SqlParameter>());
                var v = ((string)ds.Tables[0].Rows[0][0] + string.Empty).Trim().ToLower();
                return v.Contains("enterprise") || v.Contains("developer");
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return false;
            }
        }

        #endregion

        #region SupportsMemOpt

        public static bool SupportsMemOpt(string connectionString)
        {
            try
            {
                //Give small timeout
                var c = new SqlConnectionStringBuilder(connectionString);
                c.ConnectTimeout = 8;
                connectionString = c.ToString();

                //Server must support
                var ds = GetDataset(connectionString, "select SERVERPROPERTY ('IsXTPSupported')", new List<SqlParameter>());
                var retval = ((int)ds.Tables[0].Rows[0][0]) == 1;

                //This database must have been created to support
                ds = GetDataset(connectionString, "if exists(select * from sys.filegroups where type_desc = 'MEMORY_OPTIMIZED_DATA_FILEGROUP') select 1 else select 0", new List<SqlParameter>());
                retval = retval & (((int)ds.Tables[0].Rows[0][0]) == 1);

                return retval;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return false;
            }
        }

        #endregion

        #region CanConnect

        public static bool CanConnect(string connectionString)
        {
            try
            {
                var b = IsEnterpiseVersion(connectionString);
                return true;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return false;
            }
        }

        #endregion

        #region HasFTS

        public static bool HasFTS(string connectionString)
        {
            //Give small timeout
            var c = new SqlConnectionStringBuilder(connectionString);
            c.ConnectTimeout = 8;
            connectionString = c.ToString();

            var ds = GetDataset(connectionString, "SELECT FULLTEXTSERVICEPROPERTY('IsFullTextInstalled')",
                new List<SqlParameter>());
            return ((int)ds.Tables[0].Rows[0][0] != 0);
        }

        #endregion

        #region GetTableName

        private static string GetTableName(RepositorySchema schema)
        {
            return GetTableName(schema.ID);
        }

        internal static string GetTableName(Guid repositoryKey)
        {
            return $"Z_{repositoryKey.ToString().ToUpper()}";
        }

        internal static string GetTableViewName(Guid repositoryKey)
        {
            return $"__view_Z_{repositoryKey.ToString().ToUpper()}";
        }

        internal static string GetListTableName(Guid repositoryKey, long didx)
        {
            return $"Y_{repositoryKey.ToString().ToUpper()}_{didx}";
        }

        internal static string GetDimensionTableName(Guid repositoryKey)
        {
            return $"W_{repositoryKey.ToString().ToUpper()}";
        }

        internal static string GetDimensionValueTableName(Guid repositoryKey)
        {
            return $"X_{repositoryKey.ToString().ToUpper()}";
        }

        internal static string GetUserPermissionTableName(Guid repositoryKey)
        {
            return $"V_{repositoryKey.ToString().ToUpper()}";
        }

        #endregion

        #region GetIndexName

        internal static string GetIndexName(FieldDefinition field, string dataTable, bool isDimension = false)
        {
            return ("IDX_" + dataTable + "_" + (isDimension ? "__d" : string.Empty) + field.TokenName).ToUpper();
        }

        private static string GetIndexPivotName(List<string> pivotFieldList, string dataTable)
        {
            return ($"IDX_{dataTable}_{pivotFieldList.ToStringList("_")}").ToUpper();
        }

        #endregion

        #region GetSqlFTS

        private static string GetSqlRemoveFTS(string dataTable)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"if exists(select * from sys.fulltext_indexes where object_id = (select top 1 object_id from sys.objects where name = '{dataTable}'))");
            sb.AppendLine($"DROP FULLTEXT INDEX ON [{dataTable}];");
            LoggerCQ.LogInfo($"DROP FTS: {dataTable}");
            return sb.ToString();
        }

        internal static string[] GetSqlFTS(string dataTable, RepositorySchema schema)
        {
            var retval = new List<string>();
            retval.Add(GetSqlRemoveFTS(dataTable));

            RepositorySchema realSchema = null;
            if (schema.ParentID != null)
            {
                var parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);
                realSchema = schema.Subtract(parentSchema);
            }
            else realSchema = schema;

            var sb = new StringBuilder();
            var columns = GetFTSColumns(realSchema, false);
            if (!string.IsNullOrEmpty(columns))
            {
                //Create catalog if not exists
                var catalogName = $"FTS_{dataTable}";
                sb.AppendLine($"if not exists(SELECT fulltext_catalog_id, name FROM sys.fulltext_catalogs where name = '{catalogName}')");
                sb.AppendLine($"CREATE FULLTEXT CATALOG [{catalogName}] ON FILEGROUP [PRIMARY] WITH ACCENT_SENSITIVITY = OFF;");
                retval.Add(sb.ToString());
                sb = new StringBuilder();

                //Create index in that catalog above
                sb.AppendLine($"if exists(SELECT * from sys.objects where name = '{dataTable}' and type = 'U')");
                sb.AppendLine($"CREATE FULLTEXT INDEX ON [{dataTable}] ({columns})");
                sb.AppendLine($"KEY INDEX [PK_{dataTable}]");
                sb.AppendLine($"ON [{catalogName}] WITH STOPLIST = DatastoreStopList, CHANGE_TRACKING AUTO;"); //MANUAL
                retval.Add(sb.ToString());
            }
            return retval.ToArray();
        }

        #endregion

        #region ClearCache

        public static void ClearCache(string connectionString)
        {
            try
            {
                //Only run if necessary
                if (!ConfigHelper.AllowQueryCacheClearing) return;
                ExecuteSql(connectionString, "DBCC FreeProcCache");
                LoggerCQ.LogDebug("Database ClearCache");
            }
            catch (Exception ex)
            {
                LoggerCQ.LogWarning(ex);
            }
        }

        #endregion

        #region LogRepositoryStats

        public static void LogRepositoryStats(string connectionString)
        {
            var timer = Stopwatch.StartNew();
            try
            {
                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                {
                    var list = context.Repository.Select(x => x.UniqueKey).ToList();
                    foreach (var r in list)
                    {
                        var tableName = GetTableName(r);
                        var sql = "select count(*) from [" + tableName + "] " + NoLockText();
                        var ds = GetDataset(connectionString, sql);
                        if (ds.Tables.Count == 1 && ds.Tables[0].Rows.Count == 1)
                        {
                            var count = (int)ds.Tables[0].Rows[0][0];
                            ExecuteSql(connectionString, "update [Repository] set [ItemCount] = " + count + " where [UniqueKey] = '" + r + "'");
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
                timer.Stop();
                LoggerCQ.LogDebug("LogRepositoryStats: Elapsed=" + timer.ElapsedMilliseconds);
            }
        }

        #endregion

        #region DefragFTS

        public static void DefragFTS(string connectionString)
        {
            if (!ConfigHelper.DefragIndexes) return;
            if (_isDefragging) return;
            if (DateTime.Now.Date.DayOfWeek != DayOfWeek.Saturday) return;

            _isDefragging = true;
            var timer = Stopwatch.StartNew();
            try
            {
                LoggerCQ.LogDebug($"DefragFTS Start: Processor={SystemCore.LastProcessor}");
                using (var context = new DatastoreEntities(connectionString))
                {
                    var repositoryKeyList = context.Repository.Where(x => !x.IsDeleted && x.IsInitialized).Select(x => x.UniqueKey).ToList();
                    foreach (var id in repositoryKeyList)
                    {
                        var dataTable = GetTableName(id);
                        var catalogName = $"FTS_{dataTable}";

                        var sql = $"ALTER FULLTEXT CATALOG [{catalogName}] REORGANIZE";
                        ExecuteSql(connectionString, sql, null, false, false);

                        //Do not do this longer than 3 hours
                        if (timer.Elapsed.TotalHours >= 3) return;
                    }
                }
            }
            catch (System.Data.SqlClient.SqlException ex)
            {
                if (ex.ToString().Contains("Timeout Expired"))
                    LoggerCQ.LogWarning($"DefragIndexes: Timeout Expired");
                else
                    LoggerCQ.LogWarning(ex);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogWarning(ex);
            }
            finally
            {
                timer.Stop();
                LoggerCQ.LogDebug($"DefragFTS: Elapsed={timer.ElapsedMilliseconds}");
                _isDefragging = false;
            }
        }

        #endregion

        #region DefragIndexes

        public static void DefragIndexes(string connectionString)
        {
            if (!ConfigHelper.DefragIndexes) return;
            if (_isDefragging) return;
            if (SystemCore.LastProcessor >= SystemCore.ProcessorThreshold)
            {
                LoggerCQ.LogDebug($"DefragIndexes Skipped: Processor={SystemCore.LastProcessor}");
                return;
            }

            _isDefragging = true;
            var count = 0;
            var timer = Stopwatch.StartNew();
            try
            {
                ConfigHelper.LastDefrag = DateTime.Now;
                LoggerCQ.LogDebug($"DefragIndexes Start: Processor={SystemCore.LastProcessor}");

                var startTime = DateTime.Now;

                var sb = new StringBuilder();
                sb.AppendLine("SELECT OBJECT_NAME(ind.OBJECT_ID) AS TableName, ");
                sb.AppendLine("    ind.name AS IndexName, indexstats.index_type_desc AS IndexType, indexstats.page_count, indexstats.avg_fragmentation_in_percent");
                sb.AppendLine("FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) indexstats ");
                sb.AppendLine("INNER JOIN sys.indexes ind  ");
                sb.AppendLine("    ON ind.object_id = indexstats.object_id ");
                sb.AppendLine("AND ind.index_id = indexstats.index_id ");
                sb.AppendLine("WHERE indexstats.avg_fragmentation_in_percent > 30 AND ");
                sb.AppendLine("    ind.name IS NOT NULL AND ");
                sb.AppendLine("    (OBJECT_NAME(ind.OBJECT_ID) like 'Z_%' OR OBJECT_NAME(ind.OBJECT_ID) like 'Y_%') AND ");
                sb.AppendLine("    (indexstats.page_count > 500) AND ");
                sb.AppendLine("    ((indexstats.index_type_desc = 'NONCLUSTERED INDEX') OR (indexstats.index_type_desc = 'CLUSTERED INDEX'))");
                sb.AppendLine("ORDER BY indexstats.page_count");
                sb.AppendLine();

                var builder = new SqlConnectionStringBuilder(connectionString);
                var databaseName = builder.InitialCatalog;

                var ds = GetDataset(connectionString, sb.ToString(), timeOut: 120, maxRetry: 0);
                if (ds.Tables.Count != 1) return;

                //Look through all indexes and defrag
                var processed = 0;
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    var timer2 = new Stopwatch();
                    timer2.Start();
                    var tableName = (string)row[0] + string.Empty;
                    var indexName = (string)row[1] + string.Empty;

                    Guid.TryParse(tableName.Split(new char[] { '_' }).Where(x => x.Length == 36).FirstOrDefault(), out Guid repositoryId);
                    if (!ModifiedInThreshold(repositoryId, UpdateStatsThreshold))
                    {
                        try
                        {
                            //Run SQL with 30 minute timeout
                            var results = GetDataset(connectionString,
                                $"DBCC INDEXDEFRAG ([{databaseName}], '{tableName}', [{indexName}])", new List<SqlParameter>(),
                                timeOut: 1800, maxRetry: 0);
                            timer2.Stop();

                            long pagesMoved = -1;
                            long pagesRemoved = -1;
                            if (results.Tables.Count == 1)
                            {
                                pagesMoved = (long)results.Tables[0].Rows[0][1];
                                pagesRemoved = (long)results.Tables[0].Rows[0][2];
                            }

                            processed++;
                            LoggerCQ.LogDebug($"DefragIndex: Name={indexName}, Pages={((long)row[3])}, Fragmentation={((double)row[4]).ToString("0")}, PagesMoved={pagesMoved}, PagesRemoved={pagesRemoved}, Elapsed={timer2.ElapsedMilliseconds}, Processed={processed}/{ds.Tables[0].Rows.Count}");
                        }
                        catch (Exception ex)
                        {
                            //Log error and move to next index
                            LoggerCQ.LogError(ex, $"TableName={tableName}, IndexName={indexName}");
                        }
                    }

                    //If running longer than 3 hours then stop
                    if (DateTime.Now.Subtract(startTime).TotalMinutes > 180)
                    {
                        timer.Stop();
                        LoggerCQ.LogDebug($"DefragIndexes: Action=MaxTimeExceeded, Processed={processed}, Count={ds.Tables[0].Rows.Count}, Elapsed={timer.ElapsedMilliseconds}");
                        return;
                    }
                }
                count = ds.Tables[0].Rows.Count;
            }
            catch (System.Data.SqlClient.SqlException ex)
            {
                if (ex.ToString().Contains("Timeout Expired"))
                    LoggerCQ.LogWarning($"DefragIndexes: Timeout Expired");
                else
                    LoggerCQ.LogWarning(ex);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogWarning(ex);
            }
            finally
            {
                timer.Stop();
                LoggerCQ.LogDebug($"DefragIndexes: Count={count}, Elapsed={timer.ElapsedMilliseconds}");
                _isDefragging = false;
            }
        }

        #endregion

        #region CleanLogs

        public static void CleanLogs(string connectionString)
        {
            if (SystemCore.LastProcessor >= SystemCore.ProcessorThreshold)
            {
                LoggerCQ.LogDebug($"CleanLogs Skipped: Processor={SystemCore.LastProcessor}");
                return;
            }

            _isDefragging = true;
            var timer = Stopwatch.StartNew();
            var count = 0;
            try
            {
                LoggerCQ.LogDebug($"CleanLogs Start: Processor={SystemCore.LastProcessor}");

                //Keep N days of logs
                var days = ConfigHelper.LogHistoryDays;
                if (days <= 0) days = 1;
                 var pivotDate = DateTime.Now.Date.AddDays(-days).ToString("yyyy-MM-dd");

                var sb = new StringBuilder();
                sb.AppendLine($"SET ROWCOUNT {DeleteBlockSize}");
                sb.AppendLine($"DELETE FROM [RepositoryLog] WHERE [CreatedDate] < '{pivotDate}'");
                var tempCount = -1;
                while (tempCount != 0)
                {
                    RetryHelper.DefaultRetryPolicy(5)
                        .Execute(() =>
                        {
                            tempCount = ExecuteSql(connectionString, sb.ToString(), null, false);
                            count += tempCount;
                        });
                }

                tempCount = -1;
                sb = new StringBuilder();
                sb.AppendLine($"SET ROWCOUNT {DeleteBlockSize}");
                sb.AppendLine($"DELETE FROM [RepositoryStat] WHERE [CreatedDate] < '{pivotDate}'");
                while (tempCount != 0)
                {
                    RetryHelper.DefaultRetryPolicy(5)
                        .Execute(() =>
                        {
                            tempCount = ExecuteSql(connectionString, sb.ToString(), null, false);
                            count += tempCount;
                        });
                }

                tempCount = -1;
                sb = new StringBuilder();
                sb.AppendLine($"SET ROWCOUNT {DeleteBlockSize}");
                sb.AppendLine($"DELETE FROM [ServerStat] WHERE [AddedDate] < '{pivotDate}'");
                while (tempCount != 0)
                {
                    RetryHelper.DefaultRetryPolicy(5)
                        .Execute(() =>
                        {
                            tempCount = ExecuteSql(connectionString, sb.ToString(), null, false);
                            count += tempCount;
                        });
                }

                tempCount = -1;
                sb = new StringBuilder();
                sb.AppendLine($"SET ROWCOUNT 5000");
                sb.AppendLine($"DELETE FROM [CacheInvalidate] WHERE [AddedDate] < '{pivotDate}'");
                while (tempCount != 0)
                {
                    RetryHelper.DefaultRetryPolicy(5)
                        .Execute(() =>
                        {
                            tempCount = ExecuteSql(connectionString, sb.ToString(), null, false);
                            count += tempCount;
                        });
                }

            }
            catch (Exception ex)
            {
                LoggerCQ.LogWarning(ex);
            }
            finally
            {
                timer.Stop();
                LoggerCQ.LogDebug($"CleanLogs: Count={count}, Processor={SystemCore.LastProcessor}, Elapsed={timer.ElapsedMilliseconds}");
                _isDefragging = false;
            }
        }

        #endregion

        #region GetFTSColumns

        private static string GetFTSColumns(RepositorySchema schema, bool withPrefix, bool useSchemaAsIs = false)
        {
            var realSchema = schema;
            if (schema.ParentID != null && !useSchemaAsIs)
            {
                var parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);
                realSchema = schema.Subtract(parentSchema);
            }

            return GetPrimaryTableFields(realSchema, true)
                .Where(x => x.AllowTextSearch)
                .Select(x => x.TokenName)
                .Distinct()
                .Select(x => (withPrefix ? "Z." : string.Empty) + $"[{x.ReplaceSqlTicks()}]")
                .ToList()
                .ToCommaList();
        }

        #endregion

        #region UpdateStatistics

        public static void UpdateStatistics(Guid repositoryId)
        {
            var timer = Stopwatch.StartNew();
            var dataTable = GetTableName(repositoryId);
            var sb = new StringBuilder();
            sb.AppendLine($"if exists (select * from sys.objects where name = '{dataTable}' and type = 'U')");
            sb.AppendLine($"UPDATE STATISTICS [{dataTable}]");
            ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), null, false);
            timer.Stop();
            LoggerCQ.LogDebug($"UPDATE STATISTICS: ID={repositoryId}, Elapsed={timer.ElapsedMilliseconds}");
        }

        #endregion

        #region UpdateDimensionValue

        public static bool UpdateDimensionValue(RepositorySchema schema, List<DimensionItem> dimensionList, long dvidx, string value)
        {
            if (string.IsNullOrEmpty(value)) return false;

            var r = dimensionList.SelectMany(x => x.RefinementList).FirstOrDefault(x => x.DVIdx == dvidx);
            if (r == null) return false;
            var d = dimensionList.FirstOrDefault(x => x.RefinementList.Any(z => z.DVIdx == dvidx));
            if (d == null) return false;

            var sb = new StringBuilder();
            var dDef = schema.DimensionList.FirstOrDefault(x => x.DIdx == d.DIdx);
            var parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter { DbType = DbType.String, IsNullable = false, ParameterName = "@v", Value = value });
            parameters.Add(new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@dvidx", Value = dvidx });

            var dataTable = GetTableName(schema);
            var dimensionValueTableName = GetDimensionValueTableName(schema.ID);
            if (schema.ParentID != null)
            {
                var parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);
                if (parentSchema.DimensionList.Any(x => x.DIdx == dDef.DIdx))
                {
                    dataTable = GetTableName(schema.ParentID.Value);
                    dimensionValueTableName = SqlHelper.GetDimensionValueTableName(schema.ParentID.Value);
                }
            }

            //Update the Dimension Value table
            sb.AppendLine($"update [{dimensionValueTableName}] set [Value] = @v where [DVIdx] = @dvidx");

            //Update the values in the master table
            if (dDef != null && dDef.DimensionType == RepositorySchema.DimensionTypeConstants.Normal)
            {
                sb.AppendLine($"update [{dataTable}] set [{dDef.TokenName}] = @v where [__d{dDef.TokenName}] = @dvidx");
            }

            ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), parameters);

            return true;
        }

        #endregion

        #region DeleteDimensionValue

        public static bool DeleteDimensionValue(RepositorySchema schema, List<DimensionItem> dimensionList, long dvidx)
        {
            var r = dimensionList.SelectMany(x => x.RefinementList).FirstOrDefault(x => x.DVIdx == dvidx);
            if (r == null) return false;
            var d = dimensionList.FirstOrDefault(x => x.RefinementList.Any(z => z.DVIdx == dvidx));
            if (d == null) return false;

            var sb = new StringBuilder();
            var dDef = schema.DimensionList.FirstOrDefault(x => x.DIdx == d.DIdx);
            var parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@dvidx", Value = dvidx });

            //Delete the Dimension Value table
            {
                var dimensionValueTableName = GetDimensionValueTableName(schema.ID);
                sb.AppendLine($"delete from [{dimensionValueTableName}] where [DVIdx] = @dvidx");
            }

            //Delete the values in the master table
            if (dDef != null && dDef.DimensionType == RepositorySchema.DimensionTypeConstants.Normal)
            {
                var dataTable = GetTableName(schema);
                sb.AppendLine($"update [{dataTable}] set [{dDef.TokenName}] = NULL where [__d{dDef.TokenName}] = @dvidx");
                sb.AppendLine($"update [{dataTable}] set [__d{dDef.TokenName}] = NULL where [__d{dDef.TokenName}] = @dvidx");
            }
            else if (dDef != null && dDef.DimensionType == RepositorySchema.DimensionTypeConstants.List)
            {
                var dataListTable = GetListTableName(schema.ID, dDef.DIdx);
                sb.AppendLine($"delete from [{dataListTable}] where [DVIdx] = @dvidx");
            }

            ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), parameters);

            return true;
        }

        #endregion

        #region CalculateSlice
        public static SummarySliceValue CalculateSlice(RepositorySchema schema, int repositoryId, SummarySlice slice, List<DimensionItem> dimensionList, string connectionString)
        {
            var retval = new SummarySliceValue();
            retval.RecordList = new List<SummarySliceRecord>();
            retval.Definition = slice;

            if (schema.ParentID != null)
                throw new Exception("Unsupported repository with a parent");

            var errors = new List<string>();

            var sb = new StringBuilder();
            var fields = new List<FieldDefinition>();
            foreach (var f in slice.GroupFields)
            {
                var field = schema.FieldList.FirstOrDefault(x => x.Name.Match(f) && x.DataType != RepositorySchema.DataTypeConstants.List);
                if (field == null) errors.Add($"The field '{f}' is not valid.");
                else fields.Add(field);
            }

            if (errors.Count > 0)
            {
                retval.ErrorList = errors.ToArray();
                return retval;
            }
            if (fields.Count == 0)
            {
                retval.ErrorList = new string[] { "There are no valid group by fields." };
                return retval;
            }

            //If use name then calculate the DIdx
            if (slice.SpliceDIdx == 0 && !string.IsNullOrEmpty(slice.SpliceDName))
            {
                slice.SpliceDIdx = dimensionList.Where(x => x.Name.Match(slice.SpliceDName)).Select(x => x.DIdx).FirstOrDefault();
            }

            var dimension = dimensionList.FirstOrDefault(x => x.DIdx == slice.SpliceDIdx);
            if (dimension == null || schema.FieldList.Any(x => x.Name.Match(dimension.Name) && x.DataType == RepositorySchema.DataTypeConstants.List))
            {
                retval.ErrorList = new string[] { "The splice dimension is not valid." };
                return retval;
            }

            var dataTable = GetTableName(schema);
            if (schema.ParentID != null)
                dataTable = GetTableViewName(schema.ID);

            var parameters = new List<SqlParameter>();
            var whereClause = GetWhereClause(schema, null, slice.Query, dimensionList, parameters);
            var innerJoinClause = GetInnerJoinClause(schema, null, slice.Query, dimensionList, parameters);

            var allFields = fields.Select(x => x.Name).ToList();
            sb.Append("SELECT " + string.Join(",", fields.Select(x => $"[A].[{x.TokenName}]")));
            var joinFields = string.Join(" AND ", fields.Select(x => $"[A].[{x.TokenName}] = [Z].[{x.TokenName}]"));
            foreach (var refinement in dimension.RefinementList)
            {
                sb.AppendLine("	,(");
                sb.AppendLine($"		select count([Z].[{RecordIdxField}])");
                sb.AppendLine($"		from [{dataTable}] [Z] {NoLockText()}{innerJoinClause}");
                sb.AppendLine($"		where {whereClause}AND {joinFields}");
                sb.AppendLine($"			and [Z].__d{Utilities.DbTokenize(dimension.Name)} = {refinement.DVIdx}");
                sb.AppendLine($"	) as [{refinement.DVIdx}]");
                allFields.Add(refinement.DVIdx.ToString());
            }

            sb.AppendLine($"FROM [{dataTable}] [A] {NoLockText()} {innerJoinClause.Replace("[Z].", "[A].")}");
            sb.AppendLine("WHERE " + whereClause.Replace("[Z].", "[A]."));
            sb.AppendLine("GROUP BY " + fields.Select(x => $"[{x.TokenName}]").ToCommaList());

            var orderByClause = string.Empty;
            #region Order By
            if (slice.Query.FieldSorts.Count() == 0)
                orderByClause = "ORDER BY " + fields.Select(x => $"[T].[{x.TokenName}]").ToCommaList();
            else
            {
                var subSort = new List<Tuple<string, SortDirectionConstants>>();
                foreach (var f in slice.Query.FieldSorts)
                {
                    if (fields.Any(x => x.Name.Match(f.Name)))
                        subSort.Add(new Tuple<string, SortDirectionConstants>(f.Name, f.SortDirection));
                    else
                    {
                        if (!long.TryParse(f.Name, out long dvidx))
                            throw new Exception($"The order by column '{f.Name}' is not valid.");
                        if (!dimension.RefinementList.Any(x => x.DVIdx == dvidx))
                            throw new Exception($"The order by column '{f.Name}' is not valid.");
                        subSort.Add(new Tuple<string, SortDirectionConstants>(dvidx.ToString(), f.SortDirection));
                    }
                }
                orderByClause = "ORDER BY " + subSort.Select(x => $"[{x.Item1}] {x.Item2.ToSqlDirection()}").ToCommaList();
            }
            #endregion

            #region Setup paging variables
            var startIndex = ((slice.Query.PageOffset - 1) * slice.Query.RecordsPerPage) + 1;
            if (startIndex <= 0) startIndex = 1;
            var endIndex = (startIndex + slice.Query.RecordsPerPage);
            if (endIndex < 0) endIndex = int.MaxValue;
            parameters.Add(new SqlParameter { DbType = DbType.Int32, ParameterName = "@startindex", Value = startIndex });
            parameters.Add(new SqlParameter { DbType = DbType.Int32, ParameterName = "@endindex", Value = endIndex });
            #endregion

            if (ConfigHelper.SupportsRowsFetch)
            {
                var sbSql = new StringBuilder();
                sbSql.AppendLine("WITH T (" + string.Join(",", allFields.Select(x => $"[{x}]")) + ") AS (");
                sbSql.AppendLine(sb.ToString());
                sbSql.AppendLine(") SELECT * FROM T");
                sbSql.AppendLine(orderByClause);
                sbSql.AppendLine("OFFSET (@startindex-1) ROWS FETCH FIRST (@endindex-@startindex) ROWS ONLY;");
                sb = sbSql;
            }
            else
            {
                throw new Exception("Database row fetch must be supported to use this functionality.");
            }

            #region Count
            sb.AppendLine("; WITH T ([Count]) AS (");
            sb.AppendLine("select count(*) as [Count]");
            sb.AppendLine($"from [{dataTable}] Z {NoLockText()}{innerJoinClause}");
            sb.AppendLine($"where {whereClause}");
            sb.AppendLine("GROUP BY " + string.Join(",", fields.Select(x => $"[{x.TokenName}]")));
            sb.AppendLine(") select count(*) from T");

            #endregion

            var ds = GetDataset(connectionString, sb.ToString(), parameters);
            if (ds.Tables.Count != 2) throw new Exception("There were no results");
            foreach (DataRow dr in ds.Tables[0].Rows)
            {
                var record = new SummarySliceRecord();
                #region Group by Fields
                foreach (var f in fields)
                {
                    if (dr[f.Name] == System.DBNull.Value)
                    {
                        record.FieldValues.Add(null);
                    }
                    else
                    {
                        switch (f.DataType)
                        {
                            case RepositorySchema.DataTypeConstants.Bool:
                                record.FieldValues.Add(((bool)dr[f.Name]).ToString());
                                break;
                            case RepositorySchema.DataTypeConstants.DateTime:
                                record.FieldValues.Add(((DateTime)dr[f.Name]).ToString());
                                break;
                            case RepositorySchema.DataTypeConstants.GeoCode:
                                //Not supported
                                break;
                            case RepositorySchema.DataTypeConstants.Int:
                                record.FieldValues.Add(((int)dr[f.Name]).ToString());
                                break;
                            case RepositorySchema.DataTypeConstants.Int64:
                                record.FieldValues.Add(((long)dr[f.Name]).ToString());
                                break;
                            case RepositorySchema.DataTypeConstants.List:
                                //Not supported
                                break;
                            case RepositorySchema.DataTypeConstants.String:
                                record.FieldValues.Add((string)dr[f.Name]);
                                break;
                        }
                    }
                }
                #endregion

                #region Slices
                foreach (var refinement in dimension.RefinementList)
                {
                    record.SliceValues.Add(new RefinementItem
                    {
                        Count = ((int)dr[refinement.DVIdx.ToString()]),
                        DVIdx = refinement.DVIdx,
                        DIdx = refinement.DIdx,
                        FieldValue = refinement.FieldValue
                    });
                }
                #endregion
                retval.RecordList.Add(record);
                retval.TotalRecordCount = (int)ds.Tables[1].Rows[0][0];
            }
            return retval;
        }
        #endregion

        #region Private Methods

        internal static List<SqlIndex> GetTableIndexes(string connectionString, string table)
        {
            var retval = new List<SqlIndex>();
            var sb = new StringBuilder();
            sb.AppendLine("SELECT I.NAME IndexName,C.NAME ColumnName");
            sb.AppendLine("  FROM SYS.TABLES T");
            sb.AppendLine("       INNER JOIN SYS.SCHEMAS S");
            sb.AppendLine("    ON T.SCHEMA_ID = S.SCHEMA_ID");
            sb.AppendLine("       INNER JOIN SYS.INDEXES I");
            sb.AppendLine("    ON I.OBJECT_ID = T.OBJECT_ID");
            sb.AppendLine("       INNER JOIN SYS.INDEX_COLUMNS IC");
            sb.AppendLine("    ON IC.OBJECT_ID = T.OBJECT_ID");
            sb.AppendLine("       INNER JOIN SYS.COLUMNS C");
            sb.AppendLine("    ON C.OBJECT_ID  = T.OBJECT_ID");
            sb.AppendLine("   AND IC.INDEX_ID    = I.INDEX_ID");
            sb.AppendLine("   AND IC.COLUMN_ID = C.COLUMN_ID");
            sb.AppendLine($" WHERE T.Name = '{table}' and");
            sb.AppendLine("	I.Name not like 'PK_%'");
            sb.AppendLine("ORDER BY I.NAME,I.INDEX_ID,IC.KEY_ORDINAL");

            var list = GetDataset(connectionString, sb.ToString())
                .ToSqlList<string, string>("IndexName", "ColumnName");

            foreach (var item in list)
            {
                if (!retval.Any(x => x.Name == item.Item1))
                    retval.Add(new SqlIndex { Name = item.Item1 });
                retval.First(x => x.Name == item.Item1).Columns.Add(item.Item2);
            }
            return retval;
        }

        private static string GetDropView(Guid id)
        {
            var viewName = GetTableViewName(id);

            //Drop view if exists
            var sb = new StringBuilder();
            sb.AppendLine($"if exists (select * from sys.objects where name = '{viewName}' and type = 'V')");
            sb.AppendLine($"drop view [{viewName}]");
            return sb.ToString();
        }

        public static void CreateView(RepositorySchema schemaPart, RepositorySchema fullSchema, string connectionString)
        {
            var viewName = GetTableViewName(schemaPart.ID);

            //Drop view if exists
            ExecuteSql(connectionString, GetDropView(schemaPart.ID));

            //Create indexed view
            var sb = new StringBuilder();
            sb.AppendLine($"CREATE VIEW [{viewName}]");
            sb.AppendLine("WITH SCHEMABINDING");
            sb.AppendLine("AS");
            sb.Append($"select Z1.[{RecordIdxField}], Z1.[{TimestampField}], Z1.[{HashField}]");

            //In case there are duplicates, should never happen
            var fieldList = fullSchema.FieldList.Where(x => x.DataType != RepositorySchema.DataTypeConstants.List)
                .Distinct(new DistinctFieldComparer())
                .ToList();

            foreach (var f in fieldList)
            {
                if (schemaPart.FieldList.Any(x => x.Name == f.Name))
                {
                    sb.Append($",Z2.[{f.TokenName}]");
                    if (f is DimensionDefinition)
                        sb.Append($",Z2.[__d{f.TokenName}]");
                }
                else
                {
                    sb.Append($",Z1.[{f.TokenName}]");
                    if (f is DimensionDefinition)
                        sb.Append($",Z1.[__d{f.TokenName}]");
                }
            }
            sb.AppendLine();
            sb.AppendLine($"from dbo.[{GetTableName(schemaPart.ParentID.Value)}] Z1 INNER JOIN dbo.[{GetTableName(schemaPart.ID)}] Z2");
            sb.AppendLine($"ON Z1.{RecordIdxField} = Z2.{RecordIdxField}");
            ExecuteSql(connectionString, sb.ToString(), null, true, true);

            //Create index on view for FTS
            sb = new StringBuilder();
            sb.AppendLine($"CREATE UNIQUE CLUSTERED INDEX [IDX{viewName}] ON [{viewName}] ({RecordIdxField})");
            ExecuteSql(connectionString, sb.ToString());

            //Create FTS on it
            sb = new StringBuilder();
            fullSchema = fullSchema.Clone();
            fullSchema.ParentID = null;
            var columns = GetFTSColumns(fullSchema, false);
            if (!string.IsNullOrEmpty(columns))
            {
                sb.AppendLine($"CREATE FULLTEXT INDEX ON [{viewName}] ({columns})");
                sb.AppendLine($"KEY INDEX [IDX{viewName}]");
                sb.AppendLine($"ON [DatastoreFTS] WITH STOPLIST = SYSTEM, CHANGE_TRACKING AUTO;");
                ExecuteSql(connectionString, sb.ToString(), null, false);
            }
        }

        private static bool? GetValueBool(object o)
        {
            try
            {
                if (o is bool?) return (bool?)o;
                else if (o is string)
                {
                    var v = ((string)o + string.Empty).ToLower();
                    if (v == "1" || v == "true") return true;
                    if (v == "0" || v == "false") return false;
                }
            }
            catch (Exception ex) { }
            return null;
        }

        private static double? GetValueDouble(object o)
        {
            try
            {
                if (o is double?) return (double?)o;
                if (o is float?) return (float?)o;
                if (o is Single?) return (Single?)o;
                if (o is int?) return (int?)o;
                if (o is long?) return (long?)o;
                if (o is short?) return (short?)o;
                if (o is byte?) return (byte?)o;
                else if (o is string)
                {
                    var v = ((string)o + string.Empty).ToLower();
                    if (double.TryParse(v, out double d))
                        return d;
                    else return null;
                }
            }
            catch (Exception ex) { }
            return null;
        }

        private static int? GetValueInt(object o)
        {
            try
            {
                if (o is int?) return (int?)o;
                else if (o is long?) return (int?)(long?)o;
                else if (o is string)
                {
                    var v = ((string)o + string.Empty).ToLower();
                    if (int.TryParse(v, out int d))
                        return d;
                    else return null;
                }
            }
            catch (Exception ex) { }
            return null;
        }

        private static long? GetValueInt64(object o)
        {
            try
            {
                if (o is int?) return (long?)o;
                else if (o is long?) return (long?)o;
                else if (o is string)
                {
                    var v = ((string)o + string.Empty).ToLower();
                    if (long.TryParse(v, out long d))
                        return d;
                    else return null;
                }
            }
            catch (Exception ex) { }
            return null;
        }

        private static DateTime? GetValueDateTime(object o)
        {
            if (o is DateTime?) return (DateTime?)o;
            else if (o is string)
            {
                var v = ((string)o + string.Empty).ToLower();
                if (DateTime.TryParseExact(v, DimensionItem.DateTimeFormat, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out DateTime d))
                    return d;
                else return null;
            }
            return null;
        }

        private static string GetListTableCreate(string dimensionTable)
        {
            var sb = new StringBuilder();

            sb.AppendLine($"if not exists(select * from sys.objects where name = '{dimensionTable}' and type = 'U')");
            sb.AppendLine("BEGIN");
            sb.AppendLine($"CREATE TABLE [{dimensionTable}] (");
            sb.AppendLine($"[DVIdx] BIGINT NOT NULL, [{RecordIdxField}] BIGINT NOT NULL,");
            sb.AppendLine($"CONSTRAINT [PK_{dimensionTable}] PRIMARY KEY CLUSTERED ([{RecordIdxField}], [DVIdx])");
            sb.AppendLine(")");

            //Determine if the List tables are in a separate file group by configuration
            //If not, get a random group
            var fileGroup = RepositoryManager.GetRandomFileGroup();
            if (ConfigHelper.SetupConfig.HashListTableFileGroup)
                fileGroup = SetupConfig.YFileGroup;

            if (!string.IsNullOrEmpty(fileGroup))
            {
                sb.AppendLine($"ON [{fileGroup}];");
            }

            if (ConfigHelper.SupportsCompression)
            {
                sb.AppendLine($"ALTER TABLE [{dimensionTable}] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE);");
            }

            sb.AppendLine("END");

            //Create '__RecordIdx' index for speed (makes big difference)
            var indexName = $"IDX_{dimensionTable}_RecordIdx";
            sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}')");
            sb.AppendLine($"CREATE NONCLUSTERED INDEX [{indexName}] ON [{dimensionTable}] ([{RecordIdxField}]){GetSqlIndexFileGrouping()}");

            //Create 'DVIDX' index for speed (makes big difference)
            indexName = $"IDX_{dimensionTable}_DVIDX";
            sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}')");
            sb.AppendLine($"CREATE NONCLUSTERED INDEX [{indexName}] ON [{dimensionTable}] ([DVIdx]){GetSqlIndexFileGrouping()}");

            return sb.ToString();
        }

        internal static string GetDimensionTableCreate(Guid repositoryKey)
        {
            var sb = new StringBuilder();
            //Dimension table
            var dimensionTableName = GetDimensionTableName(repositoryKey);
            sb.AppendLine($"if not exists(select * from sys.objects where name = '{dimensionTableName}' and type = 'U')");
            sb.AppendLine("BEGIN");
            sb.AppendLine($"CREATE TABLE [{dimensionTableName}] (");
            sb.AppendLine($"[DIdx] BIGINT NOT NULL CONSTRAINT [PK_{dimensionTableName}] PRIMARY KEY CLUSTERED ([DIdx])");
            sb.AppendLine(")");
            var fileGroup = RepositoryManager.GetRandomFileGroup();
            if (!string.IsNullOrEmpty(fileGroup))
            {
                sb.AppendLine($"ON [{fileGroup}];");
            }
            sb.AppendLine("END");

            //Dimension value table
            var dimensionValueTableName = GetDimensionValueTableName(repositoryKey);
            sb.AppendLine($"if not exists(select * from sys.objects where name = '{dimensionValueTableName}' and type = 'U')");
            sb.AppendLine("BEGIN");
            sb.AppendLine($"CREATE TABLE [{dimensionValueTableName}] (");
            sb.AppendLine("[DVIdx] BIGINT NOT NULL,");
            sb.AppendLine("[DIdx] BIGINT NOT NULL,");
            sb.AppendLine("[Value] NVARCHAR(500) NOT NULL");
            sb.AppendLine(")");
            if (!string.IsNullOrEmpty(fileGroup))
            {
                sb.AppendLine($"ON [{fileGroup}];");
            }

            sb.AppendLine($"if not exists(select * from sys.indexes where name = 'PK_{dimensionValueTableName}')");
            sb.AppendLine($"ALTER TABLE [{dimensionValueTableName}] ADD CONSTRAINT [PK_{dimensionValueTableName}] PRIMARY KEY NONCLUSTERED ([DIdx],[DVIdx]);");

            var indexName = $"{dimensionValueTableName}_DIDX";
            sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}')");
            sb.AppendLine($"CREATE NONCLUSTERED INDEX [{indexName}] ON [{dimensionValueTableName}] ([DIdx] ASC){GetSqlIndexFileGrouping()};");

            sb.AppendLine("END");

            return sb.ToString();
        }

        private static string GetUserPermissionTableCreate(RepositorySchema schema)
        {
            if (schema.UserPermissionField == null) return string.Empty;
            var field = schema.FieldList.FirstOrDefault(x => x.Name.Match(schema.UserPermissionField.Name));
            if (field == null) return string.Empty;
            var sb = new StringBuilder();
            var fileGroup = RepositoryManager.GetRandomFileGroup();

            var sqlLength = string.Empty;
            if (field.DataType == RepositorySchema.DataTypeConstants.String)
            {
                if (field.Length > 0 && field.Length < 450) sqlLength = $"({field.Length})";
                else return string.Empty; //cannot index a field this big
            }

            //Dimension table
            var userPermissionTableName = GetUserPermissionTableName(schema.ID);
            sb.AppendLine($"if not exists(select * from sys.objects where name = '{userPermissionTableName}' and type = 'U')");
            sb.AppendLine("BEGIN");
            sb.AppendLine($"CREATE TABLE [{userPermissionTableName}] (");
            sb.AppendLine("[UserId] INT NOT NULL,");
            sb.AppendLine($"[FKField] {field.ToSqlType()} {sqlLength} NOT NULL,");
            sb.AppendLine($"CONSTRAINT [PK_{userPermissionTableName.ToUpper()}] PRIMARY KEY NONCLUSTERED ([UserId], [FKField]){GetSqlIndexFileGrouping()}");
            sb.AppendLine(")");
            if (!string.IsNullOrEmpty(fileGroup))
            {
                sb.AppendLine($"ON [{fileGroup}];");
            }
            sb.AppendLine("END");

            return sb.ToString();
        }

        private static List<FieldDefinition> GetNonPrimaryTableFields(RepositorySchema schema)
        {
            //Get all fields that are not List Dimension fields
            return schema.FieldList
                .Where(x => x.DataType == RepositorySchema.DataTypeConstants.List)
                .Cast<DimensionDefinition>()
                .Where(x => x.DimensionType == RepositorySchema.DimensionTypeConstants.List)
                .Cast<FieldDefinition>()
                .ToList();
        }

        private static List<FieldDefinition> GetPrimaryTableFields(RepositorySchema schema, bool onlyString)
        {
            //Get all fields that are not List Dimension fields
            return schema.FieldList
                .Where(x => (x.DataType != RepositorySchema.DataTypeConstants.List) &&
                    (!onlyString || (onlyString && x.DataType == RepositorySchema.DataTypeConstants.String)))
                .ToList();
        }

        internal static string NoLockText(bool isData = true)
        {
            if (!isData) return string.Empty;
            return WITHNOLOCK_TEXT;
        }

        #endregion

        #region VerifyTablesExists

        /// <summary>
        /// If the underlying tables are missing then remove the Repository entries as they are no good anyway
        /// </summary>
        /// <param name="connectionString"></param>
        internal static void VerifyTablesExists(string connectionString)
        {
            try
            {
                using (var context = new DatastoreEntities(connectionString))
                {
                    var deletedList = new List<Tuple<int, Guid>>();
                    var repositoryKeyList = context.Repository
                        .Where(x => !x.IsDeleted && x.IsInitialized)
                        .Select(x => new { x.UniqueKey, x.RepositoryId })
                        .ToList();

                    var allTables = GetDataset(connectionString, "select name from sys.tables")
                        .Tables[0]
                        .Rows.ToList<DataRow>()
                        .Select(x => (string)x[0])
                        .ToHash();

                    foreach (var item in repositoryKeyList)
                    {
                        var dataTable = GetTableName(item.UniqueKey);
                        var dimensionTableName = GetDimensionTableName(item.UniqueKey);
                        var dimensionValueTableName = GetDimensionValueTableName(item.UniqueKey);
                        if (!allTables.Any(x => x == dataTable || x == dimensionTableName || x == dimensionValueTableName))
                        {
                            deletedList.Add(new Tuple<int, Guid>(item.RepositoryId, item.UniqueKey));
                        }
                    }

                    foreach (var item in deletedList)
                    {
                        var id = item.Item1;
                        Task.Factory.StartNew(() =>
                        {
                            Gravitybox.Datastore.EFDAL.Entity.RepositoryLog.DeleteData(x => x.RepositoryId == id, new QueryOptimizer { ChunkSize = 5000 });
                        });

                        Gravitybox.Datastore.EFDAL.Entity.Repository.DeleteData(x => x.RepositoryId == item.Item1);
                        RemoveRepository(connectionString, item.Item2);
                        LoggerCQ.LogWarning($"VerifyTablesExists: ID={item.Item2}");
                    }

                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }


        #endregion

        #region Permissions

        public static void AddPermission(RepositorySchema schema, IEnumerable<PermissionItem> list)
        {
            if (list == null) return;
            if (schema == null || schema.UserPermissionField == null)
            {
                LoggerCQ.LogDebug("Repository does not have permissions: " + schema.ID);
                return;
            }

            var userPermissionTable = GetUserPermissionTableName(schema.ID);
            var chunks = list.Chunk(500).ToList();
            foreach (var subList in chunks)
            {
                var sb = new StringBuilder();
                var parameters = new List<SqlParameter>();
                var index = 0;
                foreach (var item in subList)
                {
                    sb.AppendLine($"if not exists(select * from [{userPermissionTable}] where UserId = @UserId{index} and FKField = @Field{index})");
                    sb.AppendLine($"insert into [{userPermissionTable}] (UserId, FKField) values (@UserId{index}, @Field{index})");
                    parameters.Add(new SqlParameter { DbType = DbType.Int32, IsNullable = false, ParameterName = "@UserId" + index, Value = item.UserId });
                    if (schema.UserPermissionField.DataType == RepositorySchema.DataTypeConstants.Int)
                    {
                        if (!int.TryParse(item.FieldValue, out int v))
                            throw new Exception("Cannot convert data");
                        parameters.Add(new SqlParameter { DbType = DbType.Int32, IsNullable = false, ParameterName = "@Field" + index, Value = v });
                    }
                    else if (schema.UserPermissionField.DataType == RepositorySchema.DataTypeConstants.Int64)
                    {
                        if (!long.TryParse(item.FieldValue, out long v))
                            throw new Exception("Cannot convert data");
                        parameters.Add(new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@Field" + index, Value = v });
                    }
                    else if (schema.UserPermissionField.DataType == RepositorySchema.DataTypeConstants.String)
                        parameters.Add(new SqlParameter { DbType = DbType.String, IsNullable = false, ParameterName = "@Field" + index, Value = item.FieldValue });
                    else
                        throw new Exception("Unsupported data type!");
                    index++;
                }
                ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), parameters);
            }
        }

        public static void DeletePermission(RepositorySchema schema, IEnumerable<PermissionItem> list)
        {
            if (list == null) return;
            var userPermissionTable = GetUserPermissionTableName(schema.ID);
            var chunks = list.Chunk(500).ToList();
            foreach (var subList in chunks)
            {
                var sb = new StringBuilder();
                var parameters = new List<SqlParameter>();
                var index = 0;
                foreach (var item in list)
                {
                    sb.AppendLine($"if exists(select * from [{userPermissionTable}] where UserId = @UserId{index} and FKField = @Field{index})");
                    sb.AppendLine($"delete from [{userPermissionTable}] where UserId = @UserId{index} and FKField = @Field{index}");
                    parameters.Add(new SqlParameter { DbType = DbType.Int32, IsNullable = false, ParameterName = "@UserId" + index, Value = item.UserId });
                    if (schema.UserPermissionField.DataType == RepositorySchema.DataTypeConstants.Int)
                    {
                        if (!int.TryParse(item.FieldValue, out int v))
                            throw new Exception("Connot convert data");
                        parameters.Add(new SqlParameter { DbType = DbType.Int32, IsNullable = false, ParameterName = "@Field" + index, Value = v });
                    }
                    else if (schema.UserPermissionField.DataType == RepositorySchema.DataTypeConstants.Int64)
                    {
                        if (!long.TryParse(item.FieldValue, out long v))
                            throw new Exception("Connot convert data");
                        parameters.Add(new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = "@Field" + index, Value = v });
                    }
                    else if (schema.UserPermissionField.DataType == RepositorySchema.DataTypeConstants.String)
                        parameters.Add(new SqlParameter { DbType = DbType.String, IsNullable = false, ParameterName = "@Field" + index, Value = item.FieldValue });
                    else
                        throw new Exception("Unsupported data type!");
                    index++;
                }
                ExecuteSql(ConfigHelper.ConnectionString, sb.ToString(), parameters);
            }
        }

        public static bool NeedClearPermissions(Guid id)
        {
            //Get the count of permissions and if 0 then do not bother to run query
            if (_permissionCount.TryGetValue(id, out int count) && count == 0)
                return false;
            return true;
        }

        public static void ClearPermissions(RepositorySchema schema, string fieldValue)
        {
            if (schema == null) return;
            if (schema.UserPermissionField == null) return;

            //Get the count of permissions and if 0 then do not bother to run query
            //int count;
            //if (_permissionCount.TryGetValue(schema.ID, out count) && count == 0)
            //{
            //    LoggerCQ.LogInfo("ClearPermissions Skipout");
            //    return;
            //}
            if (!NeedClearPermissions(schema.ID))
                return;

            var userPermissionTable = GetUserPermissionTableName(schema.ID);
            if (!string.IsNullOrEmpty(fieldValue))
            {
                const string FKFieldField = "FKField";
                var parameters = new List<SqlParameter>();
                if (schema.UserPermissionField.DataType == RepositorySchema.DataTypeConstants.Int)
                {
                    if (!int.TryParse(fieldValue, out int v))
                        throw new Exception("Cannot convert data");
                    parameters.Add(new SqlParameter { DbType = DbType.Int32, IsNullable = false, ParameterName = $"@{FKFieldField}", Value = v });
                }
                else if (schema.UserPermissionField.DataType == RepositorySchema.DataTypeConstants.Int64)
                {
                    if (!long.TryParse(fieldValue, out long v))
                        throw new Exception("Cannot convert data");
                    parameters.Add(new SqlParameter { DbType = DbType.Int64, IsNullable = false, ParameterName = $"@{FKFieldField}", Value = v });
                }
                else if (schema.UserPermissionField.DataType == RepositorySchema.DataTypeConstants.String)
                    parameters.Add(new SqlParameter { DbType = DbType.String, IsNullable = false, ParameterName = $"@{FKFieldField}", Value = fieldValue });
                else
                    throw new Exception("Unsupported data type!");

                var ds = GetDataset(ConfigHelper.ConnectionString, $"delete from [{userPermissionTable}] where [{FKFieldField}] = @{FKFieldField};select count(*) from [{userPermissionTable}] {NoLockText()}", parameters);
                var count = (int)ds.Tables[0].Rows[0][0];
                _permissionCount.AddOrUpdate(schema.ID, count, (key, value) => count);
            }
            else
            {
                //There is no value so remove all items
                ExecuteSql(ConfigHelper.ConnectionString, $"truncate table [{userPermissionTable}]");
                _permissionCount.AddOrUpdate(schema.ID, 0, (key, value) => 0);
            }
        }

        public static void ClearUserPermissions(RepositorySchema schema, int userId)
        {
            if (schema == null) return;
            if (schema.UserPermissionField == null) return;

            if (!NeedClearPermissions(schema.ID))
                return;

            var userPermissionTable = GetUserPermissionTableName(schema.ID);
            var parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter { DbType = DbType.Int32, IsNullable = false, ParameterName = "@UserId", Value = userId });
            var ds = GetDataset(ConfigHelper.ConnectionString, $"delete from [{userPermissionTable}] where [UserId] = @UserId;select count(*) from [{userPermissionTable}] {NoLockText()}", parameters);
            var count = (int)ds.Tables[0].Rows[0][0];
            _permissionCount.AddOrUpdate(schema.ID, count, (key, value) => count);
        }

        #endregion

        #region SqlIndex Class

        internal class SqlIndex
        {
            public SqlIndex()
            {
                this.Columns = new List<string>();
            }

            public string Name { get; set; }
            public List<string> Columns { get; set; }
        }

        private class DistinctFieldComparer : IEqualityComparer<FieldDefinition>
        {

            public bool Equals(FieldDefinition x, FieldDefinition y)
            {
                return x.Name == y.Name;
            }

            public int GetHashCode(FieldDefinition obj)
            {
                return obj.GetHashCode();
            }

        }

        #endregion

        /// <summary>
        /// Determines if the FTS index is fully populated for a repository
        /// </summary>
        /// <param name="repositoryId"></param>
        /// <returns></returns>
        public static bool IsFTSReady(Guid repositoryId)
        {
            try
            {
                var timer = Stopwatch.StartNew();
                var dataTable = GetTableName(repositoryId);
                var sb = new StringBuilder();
                sb.AppendLine("create table #ftsmaptemp (docid int, [key] int)");
                sb.AppendLine($"declare @objectId int = OBJECT_ID('{dataTable}')");
                sb.AppendLine("insert into #ftsmaptemp (docid, [key]) exec sp_fulltext_keymappings @objectId");
                sb.AppendLine("select count(*) from #ftsmaptemp");
                sb.AppendLine($"select count(*) from [{dataTable}]");
                var ds = SqlHelper.GetDataset(ConfigHelper.ConnectionString, sb.ToString());
                var ftsCount = (int)ds.Tables[0].Rows[0][0];
                var itemCount = (int)ds.Tables[1].Rows[0][0];
                timer.Stop();
                LoggerCQ.LogDebug($"IsFTSReady: ID={repositoryId}, Elapsed={timer.ElapsedMilliseconds}");

                //If the counts match then the FTS indexing is done
                return ftsCount == itemCount;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, "IsFTSReady Error");
            }
            return false;
        }

    }

    internal class DatastoreException : System.Exception
    {
        /// <summary />
        public DatastoreException() : base() { }

        /// <summary />
        public DatastoreException(string message) : base(message) { }
    }

}