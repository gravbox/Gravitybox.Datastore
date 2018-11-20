using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;
using System.Data;
using System.Diagnostics;
using Gravitybox.Datastore.EFDAL.Entity;

namespace Gravitybox.Datastore.Server.Core
{
    internal static class PatchesDomain
    {
        /// <summary>
        /// Ensure that all list tables have an index on field '__RecordIndex'
        /// </summary>
        public static void ApplyFix_ListTableRecordIndex(string connectionString)
        {
            using (var context = new DatastoreEntities(connectionString))
            {
                var repositoryKeyList = context.Repository.Where(x => !x.IsDeleted && x.IsInitialized).Select(x => x.UniqueKey).ToList();
                var count = repositoryKeyList.Count;
                var index = 0;
                foreach (var g in repositoryKeyList)
                {
                    index++;
                    var r = context.Repository.FirstOrDefault(x => x.UniqueKey == g);
                    if (r != null)
                    {
                        var schema = new RepositorySchema();
                        schema.LoadXml(r.DefinitionData);
                        foreach (var d in schema.DimensionList.Where(x => x.DataType == RepositorySchema.DataTypeConstants.List))
                        {
                            var sb = new StringBuilder();
                            var dimensionTable = SqlHelper.GetListTableName(g, d.DIdx);
                            var indexName = $"IDX_{dimensionTable}_RecordIdx";
                            sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}')");
                            sb.AppendLine($"CREATE NONCLUSTERED INDEX [{indexName}] ON [{dimensionTable}] ([{SqlHelper.RecordIdxField}])");
                            SqlHelper.ExecuteSql(connectionString, sb.ToString());
                            LoggerCQ.LogInfo($"Apply Index: {indexName} ({index}/{count})");
                        }
                    }
                }
            }
            LoggerCQ.LogInfo("ApplyFix_ListTableRecordIndex");
        }

        public static void ApplyFix_MakePKNonClustered(string connectionString)
        {
            return;
            //try
            //{
            //    using (var context = new DatastoreEntities(connectionString))
            //    {
            //        var repositoryKeyList = context.Repository.Where(x => !x.IsDeleted && x.IsInitialized).Select(x => x.UniqueKey).ToList();
            //        var count = repositoryKeyList.Count;
            //        var index = 0;
            //        foreach (var g in repositoryKeyList)
            //        {
            //            index++;
            //            var r = context.Repository.FirstOrDefault(x => x.UniqueKey == g);
            //            if (r != null)
            //            {
            //                var schema = new RepositorySchema();
            //                schema.LoadXml(r.DefinitionData);
            //                var hasGeo = schema.FieldList.Any(x => x.DataType == RepositorySchema.DataTypeConstants.GeoCode);

            //                if (!hasGeo)
            //                {
            //                    var sb = new StringBuilder();
            //                    var dataTable = GetTableName(g);
            //                    sb.AppendLine("if exists(select * from sys.indexes where name = 'PK_" + dataTable + "' and type_desc = 'CLUSTERED')");
            //                    sb.AppendLine("BEGIN");
            //                    sb.AppendLine("if exists(select * from sys.fulltext_indexes where object_id = (select top 1 object_id from sys.objects where name = '" + dataTable + "'))");
            //                    sb.AppendLine("DROP FULLTEXT INDEX ON [" + dataTable + "];");
            //                    sb.AppendLine("ALTER TABLE [" + dataTable + "] DROP CONSTRAINT [PK_" + dataTable + "]");
            //                    sb.AppendLine("ALTER TABLE [" + dataTable + "] WITH NOCHECK ADD CONSTRAINT [PK_" + dataTable + "] PRIMARY KEY NONCLUSTERED (["+ RecordIdxField + "])");
            //                    sb.AppendLine("END");
            //                    SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false);

            //                    var sqlList = GetSqlFTS(dataTable, schema);
            //                    foreach (var s in sqlList)
            //                        SqlHelper.ExecuteSql(connectionString, s, null, false);

            //                    LoggerCQ.LogInfo("Apply NonClustered: " + dataTable + " (" + index + "/" + count + ")");
            //                }
            //            }
            //        }
            //    }
            //    LoggerCQ.LogInfo("ApplyFix_ListTableRecordIndex");
            //}
            //catch (Exception ex)
            //{
            //    LoggerCQ.LogError(ex);
            //    throw;
            //}
        }

        public static void ApplyFix_EnsureIndexes(string connectionString)
        {
            using (var context = new DatastoreEntities(connectionString))
            {
                var repositoryKeyList = context.Repository.Where(x => !x.IsDeleted && x.IsInitialized).Select(x => x.UniqueKey).ToList();
                var count = repositoryKeyList.Count;
                var index = 0;
                foreach (var g in repositoryKeyList)
                {
                    index++;
                    var r = context.Repository.FirstOrDefault(x => x.UniqueKey == g);
                    if (r != null)
                    {
                        var schema = new RepositorySchema();
                        schema.LoadXml(r.DefinitionData);
                        if (!schema.FieldList.Any(x => x.DataType == RepositorySchema.DataTypeConstants.GeoCode))
                        {
                            var indexList = new List<string>();
                            var sql = SqlHelper.GetRepositorySql(schema, indexList);
                            SqlHelper.ExecuteSql(connectionString, sql, null, false);
                            LoggerCQ.LogInfo("Apply EnsureIndexes: " + g + " (" + index + "/" + count + ")");
                        }
                    }
                }
            }
        }

        public static void ApplyFix_CompressTablesA(string connectionString)
        {
            try
            {
                if (!SqlHelper.IsEnterpiseVersion(connectionString)) return;
                SqlHelper.ExecuteSql(connectionString, "ALTER TABLE [RepositoryLog] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE);", null, false);
                SqlHelper.ExecuteSql(connectionString, "ALTER INDEX [IDX_REPOSITORYLOG_REPOSITORYID] ON [RepositoryLog] REBUILD WITH (DATA_COMPRESSION = PAGE);", null, false);
                LoggerCQ.LogInfo("ApplyFix_CompressTablesA");
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void ApplyFix_RemoveLazyDelete(string connectionString)
        {
            const string IsDeletedField = "__isDeleted_Internal";
            try
            {
                using (var context = new DatastoreEntities(connectionString))
                {
                    var repositorylist = context.Repository.Select(x => x.UniqueKey).ToList();
                    foreach (var r in repositorylist)
                    {
                        var dataTable = SqlHelper.GetTableName(r);
                        var defaultName = "DF__" + dataTable + IsDeletedField;
                        var indexName = ("IDX_" + dataTable + "_" + IsDeletedField).ToUpper();

                        var sb = new StringBuilder();
                        sb.AppendLine("if exists(select * from sys.indexes where name = '" + indexName + "')");
                        sb.AppendLine("DROP INDEX [" + indexName + "] ON [" + dataTable + "]");

                        sb.AppendLine("if exists(select * from sys.objects where name = '" + defaultName + "')");
                        sb.AppendLine("ALTER TABLE [" + dataTable + "] DROP CONSTRAINT [" + defaultName + "]");

                        sb.AppendLine("if exists(select * from sys.objects where name = '" + dataTable + "' and type = 'U') AND exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '" + IsDeletedField + "' and o.name = '" + dataTable + "')");
                        sb.AppendLine("ALTER TABLE [" + dataTable + "] DROP COLUMN [" + IsDeletedField + "]");

                        try
                        {
                            SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false);
                        }
                        catch { }
                        LoggerCQ.LogInfo("Remove LazyDelete Field [" + dataTable + "]");
                    }
                }
                LoggerCQ.LogInfo("ApplyFix_RemoveLazyDelete");
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void ApplyFix_RepositoryIntegrity(string connectionString)
        {
            //Make sure that all repository entries actually have been initialized
            //Otherwise there was a creation error like out of disk space when creating
            using (var context = new DatastoreEntities(connectionString))
            {
                var repositoryKeyList = context.Repository.Where(x => !x.IsDeleted && !x.IsInitialized).Select(x => x.UniqueKey).ToList();
                foreach (var key in repositoryKeyList)
                {
                    var dsExisting = SqlHelper.GetDataset(connectionString, "select name from sys.objects where type = 'U' and name like '%" + key + "%'");
                    var deletedList = new List<string>();
                    foreach (DataRow row in dsExisting.Tables[0].Rows)
                    {
                        deletedList.Add((string)row[0]);
                    }

                    foreach (var tableName in deletedList)
                    {
                        SqlHelper.ExecuteSql(connectionString, "if exists(select * from sys.objects where type = 'U' and name = '" + tableName + "')\r\n" +
                            "drop table [" + tableName + "]");
                    }

                    Repository.DeleteData(x => x.UniqueKey == key, connectionString);
                    LoggerCQ.LogWarning("Delete corrupt repository: ID=" + key);
                }
            }
        }

        public static void ApplyFix_LogOptimize(string connectionString)
        {
            //Make sure that all repository entries actually have the data tables present and if not remove the entry
            try
            {
                var sb = new StringBuilder();
                sb.AppendLine("if not exists(select * from sys.indexes where name = 'IDX_REPOSITORYLOG_CREATEDDATE')");
                sb.AppendLine("BEGIN");
                sb.AppendLine("CREATE NONCLUSTERED INDEX [IDX_REPOSITORYLOG_CREATEDDATE] ON [RepositoryLog] ([CreatedDate] desc);");
                if (SqlHelper.IsEnterpiseVersion(connectionString))
                    sb.AppendLine("ALTER INDEX [IDX_REPOSITORYLOG_CREATEDDATE] ON [RepositoryLog] REBUILD WITH (DATA_COMPRESSION = PAGE);");
                sb.AppendLine("END");

                if (SqlHelper.IsEnterpiseVersion(connectionString))
                {
                    sb.AppendLine("if exists(select * from sys.indexes where name = 'IDX_REPOSITORYLOG_REPOSITORYID')");
                    sb.AppendLine("ALTER INDEX [IDX_REPOSITORYLOG_REPOSITORYID] ON [RepositoryLog] REBUILD WITH (DATA_COMPRESSION = PAGE);");
                }
                SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false);

                sb.AppendLine("DELETE FROM [RepositoryLog] WHERE [CreatedDate] < '" + DateTime.Now.Date.AddDays(-30).ToString("yyyy-MM-dd") + "'");
                SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false);

                sb.AppendLine("DELETE FROM [RepositoryStat] WHERE [CreatedDate] < '" + DateTime.Now.Date.AddDays(-30).ToString("yyyy-MM-dd") + "'");
                SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void ApplyFix_DimensionTables(string connectionString)
        {
            //Create Dimension tables for each repository
            try
            {
                using (var context = new DatastoreEntities(connectionString))
                {
                    var list = context.Repository.Select(x => new { x.RepositoryId, x.UniqueKey }).ToList();

                    //Create the tables
                    var index = 0;
                    foreach (var r in list)
                    {
                        var sb = new StringBuilder();
                        sb.AppendLine(SqlHelper.GetDimensionTableCreate(r.UniqueKey));
                        SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false);
                        index++;
                        LoggerCQ.LogInfo("ApplyFix_DimensionTables: ID=" + r.UniqueKey + ", Progress:" + index + "/" + list.Count);
                    }

                    //Load the data
                    index = 0;
                    foreach (var r in list)
                    {
                        var sb = new StringBuilder();
                        var dimensionTableName = SqlHelper.GetDimensionTableName(r.UniqueKey);
                        var dimensionValueTableName = SqlHelper.GetDimensionValueTableName(r.UniqueKey);
                        sb.AppendLine("if not exists(select * from [" + dimensionTableName + "])");
                        sb.AppendLine("INSERT INTO [" + dimensionTableName + "] ([DIdx]) SELECT [DIdx] FROM [Dimension] WHERE [RepositoryId] = " + r.RepositoryId);
                        sb.AppendLine("if not exists(select * from [" + dimensionValueTableName + "])");
                        sb.AppendLine("INSERT INTO [" + dimensionValueTableName + "] ([DIdx], [DVIdx], [Value]) select d.DIdx, v.DVIdx, v.Value from [Dimension] d inner join [DimensionValue] v on D.DimensionId = v.DimensionId WHERE [RepositoryId] = " + r.RepositoryId);

                        //Delete original data
                        sb.AppendLine("DELETE FROM [DimensionValue] WHERE [DimensionId] IN (SELECT [DimensionId] FROM [Dimension] WHERE [RepositoryId] = " + r.RepositoryId + ")");
                        sb.AppendLine("DELETE FROM [Dimension] WHERE [RepositoryId] = " + r.RepositoryId);
                        SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false);
                        index++;
                    }

                    //Drop OLD Dimension tables
                    {
                        var sb = new StringBuilder();
                        sb.AppendLine("if exists(select * from sys.objects where name = 'DimensionValue' and type = 'U')");
                        sb.AppendLine("drop table [DimensionValue]");
                        sb.AppendLine("if exists(select * from sys.objects where name = 'Dimension' and type = 'U')");
                        sb.AppendLine("drop table [Dimension]");
                        SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false);
                    }

                }

            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void ApplyFix_AddTimestamp(string connectionString)
        {
            //Create Dimension tables for each repository
            try
            {
                using (var context = new DatastoreEntities(connectionString))
                {
                    var list = context.Repository.Select(x => x.UniqueKey).ToList();

                    //Create the tables
                    var index = 0;
                    foreach (var ID in list)
                    {
                        var sb = new StringBuilder();
                        var dataTable = SqlHelper.GetTableName(ID);

                        //Add Timestamp field
                        sb.AppendLine($"if exists(select * from sys.objects where name = '{dataTable}' and type = 'U') and not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '{SqlHelper.TimestampField}' and o.name = '{dataTable}')");
                        sb.AppendLine($"ALTER TABLE [{dataTable}] ADD [{SqlHelper.TimestampField}] [INT] NOT NULL CONSTRAINT [DF__{dataTable}_{SqlHelper.TimestampField}] DEFAULT 0");

                        //Index
                        var indexName = SqlHelper.GetIndexName(new FieldDefinition { Name = SqlHelper.TimestampField }, dataTable);
                        sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}')");
                        sb.AppendLine("BEGIN");
                        sb.AppendLine($"CREATE NONCLUSTERED INDEX [{indexName}] ON [{dataTable}] ([{SqlHelper.TimestampField}] ASC);");

                        if (ConfigHelper.SupportsCompression)
                            sb.AppendLine($"ALTER INDEX [{indexName}] ON [{dataTable}] REBUILD WITH (DATA_COMPRESSION = PAGE);");

                        sb.AppendLine("END");
                        sb.AppendLine();

                        SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false);
                        index++;
                        LoggerCQ.LogInfo($"ApplyFix_AddTimestamp: ID={ID}, Progress:{index}/{list.Count}");
                    }

                }

            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void ApplyFix_AddUniquePKIndex(string connectionString)
        {
            //Create Dimension tables for each repository
            try
            {
                using (var context = new DatastoreEntities(connectionString))
                {
                    var list = context.Repository.Select(x => x.UniqueKey).ToList();

                    var sb = new StringBuilder();
                    sb.AppendLine("SELECT IndexName = i.Name,");
                    sb.AppendLine("       TableName = object_name(o.object_id)");
                    sb.AppendLine("FROM   sys.indexes i");
                    sb.AppendLine("       JOIN sys.objects o");
                    sb.AppendLine("         ON i.object_id = o.object_id");
                    sb.AppendLine("WHERE  is_primary_key = 0");
                    sb.AppendLine("       AND is_unique = 1");
                    sb.AppendLine("       AND o.object_id IN (SELECT object_id");
                    sb.AppendLine("                           FROM   sys.objects");
                    sb.AppendLine("                           WHERE  TYPE = 'U')");
                    sb.AppendLine("ORDER  BY TableName,");
                    sb.AppendLine("          IndexName; ");
                    var ds = SqlHelper.GetDataset(connectionString, sb.ToString());
                    var existing = new Dictionary<string, string>();
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        var indexName = (string)dr[0];
                        var tableName = (string)dr[1];
                        existing.Add(tableName, indexName);
                    }

                    //Create the tables
                    var index = 0;
                    foreach (var ID in list)
                    {
                        var dataTable = SqlHelper.GetTableName(ID);
                        if (!existing.ContainsKey(dataTable))
                        {
                            var r = context.Repository.FirstOrDefault(x => x.UniqueKey == ID);
                            if (r != null && r.ParentId == null)
                            {
                                var schema = new RepositorySchema();
                                schema.LoadXml(r.DefinitionData);
                                var indexName = SqlHelper.GetIndexName(schema.PrimaryKey, dataTable);
                                var sql = new StringBuilder();

                                //Delete duplicates
                                sql.AppendLine("delete from [" + dataTable + "]");
                                sql.AppendLine("where [" + schema.PrimaryKey.TokenName + "] in (");
                                sql.AppendLine("select [" + schema.PrimaryKey.TokenName + "]");
                                sql.AppendLine("from  [" + dataTable + "]");
                                sql.AppendLine("group by [" + schema.PrimaryKey.TokenName + "]");
                                sql.AppendLine("having count([" + schema.PrimaryKey.TokenName + "]) > 1)");

                                //Add the index
                                sql.AppendLine("if exists (select * from sys.indexes where name = '" + indexName + "')");
                                sql.AppendLine("DROP INDEX [" + dataTable + "].[" + indexName + "]");
                                sql.AppendLine("CREATE UNIQUE NONCLUSTERED INDEX [" + indexName + "] ON [" + dataTable + "] ([" + schema.PrimaryKey.TokenName + "] ASC);");
                                try
                                {
                                    SqlHelper.ExecuteSql(connectionString, sql.ToString(), null, false);
                                }
                                catch (Exception ex)
                                {
                                    LoggerCQ.LogWarning(ex, "ApplyFix_AddUniquePKIndex: Upgrade problem. ID=" + ID);
                                }
                            }
                        }
                        index++;
                        LoggerCQ.LogInfo("ApplyFix_AddUniquePKIndex: ID=" + ID + ", Progress:" + index + "/" + list.Count);
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void ApplyFix_DVIdxMakeLong(string connectionString)
        {
            //Create Dimension tables for each repository
            try
            {
                using (var context = new DatastoreEntities(connectionString))
                {
                    var list = context.Repository.ToList();

                    var index = 0;
                    foreach (var repository in list)
                    {
                        var sb = new StringBuilder();
                        var dataTable = SqlHelper.GetTableName(repository.UniqueKey);

                        //Find all indexes for this table
                        var indexList = SqlHelper.GetTableIndexes(connectionString, dataTable);

                        var schema = RepositoryManager.GetSchema(repository.UniqueKey);
                        if (schema == null) return;
                        foreach (var dimension in schema.DimensionList)
                        {
                            //var indexName = GetIndexName(dimension, dataTable, true);
                            var dimensionColumnName = "__d" + dimension.TokenName;
                            sb.AppendLine("if exists(select * from sys.objects where name = '" + dataTable + "' and type = 'U') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '" + dimensionColumnName + "' and c.xtype = 56 and o.name = '" + dataTable + "')");
                            sb.AppendLine("BEGIN");

                            //Delete all dimension indexes for this field
                            var subIndexes = indexList.Where(x => x.Columns.Any(z => z.Match(dimensionColumnName))).ToList();
                            foreach (var name in subIndexes)
                            {
                                sb.AppendLine("if exists(select * from sys.indexes where name = '" + name.Name + "')");
                                sb.AppendLine("DROP INDEX [" + name.Name + "] ON [" + dataTable + "]");
                            }

                            sb.AppendLine("ALTER TABLE [" + dataTable + "] ALTER COLUMN [" + dimensionColumnName + "] [BIGINT] NULL");
                            sb.AppendLine("END");
                        }

                        SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false);

                        //Normalize schema now that changed all fields
                        if (schema.ParentID == null)
                        {
                            var sql = SqlHelper.GetRepositorySql(schema);
                            SqlHelper.ExecuteSql(connectionString, sql);
                        }
                        else
                        {
                            var parentSchema = RepositoryManager.GetSchema(schema.ParentID.Value);
                            var sql = SqlHelper.GetRepositorySql(schema.Subtract(parentSchema));
                            SqlHelper.ExecuteSql(connectionString, sql);
                        }

                        index++;
                        LoggerCQ.LogInfo("ApplyFix_DVIdxMakeLong: ID=" + repository.UniqueKey + ", Progress:" + index + "/" + list.Count);
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void ApplyFix_XTableMultiKey(string connectionString)
        {
            //Create Dimension tables for each repository
            try
            {
                using (var context = new DatastoreEntities(connectionString))
                {
                    var list = context.Repository.ToList();

                    var index = 0;
                    foreach (var repository in list)
                    {
                        var sb = new StringBuilder();
                        var dimensionValueTableName = SqlHelper.GetDimensionValueTableName(repository.UniqueKey);

                        sb.AppendLine($"if exists (select * from sys.indexes where name = 'PK_{dimensionValueTableName}')");
                        sb.AppendLine($"ALTER TABLE [{dimensionValueTableName}] DROP CONSTRAINT [PK_{dimensionValueTableName}]");
                        sb.AppendLine("GO");
                        sb.AppendLine($"if exists (select * from sys.objects where name = '{dimensionValueTableName}' and type = 'U')");
                        sb.AppendLine($"ALTER TABLE [{dimensionValueTableName}] ADD CONSTRAINT [PK_{dimensionValueTableName}] PRIMARY KEY NONCLUSTERED ([DVIdx] ASC,[DIdx] ASC)");
                        SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false);

                        index++;
                        LoggerCQ.LogInfo($"ApplyFix_XTableMultiKey: ID={repository.UniqueKey}, Progress:{index}/{list.Count}");
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void ApplyFix_CompressTimestamp(string connectionString)
        {
            try
            {
                var sb = new StringBuilder();
                sb.AppendLine("SELECT DISTINCT");
                sb.AppendLine("OBJECT_NAME(o.object_id) AS TableName,");
                sb.AppendLine("i.name AS IndexName");
                sb.AppendLine("FROM sys.partitions  p ");
                sb.AppendLine("INNER JOIN sys.objects o ");
                sb.AppendLine("ON p.object_id = o.object_id ");
                sb.AppendLine("JOIN sys.indexes i ");
                sb.AppendLine("ON p.object_id = i.object_id");
                sb.AppendLine("AND i.index_id = p.index_id");
                sb.AppendLine("WHERE p.data_compression = 0 and ");
                sb.AppendLine("SCHEMA_NAME(o.schema_id) <> 'SYS' ");
                //sb.AppendLine("and i.name like '%___TIMESTAMP'");
                sb.AppendLine("and i.name like 'IDX_Z_%'"); //get all uncompressed from data table
                sb.AppendLine("order by i.name");
                var ds = SqlHelper.GetDataset(connectionString, sb.ToString());

                var count = ds.Tables[0].Rows.Count;
                var index = 0;
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    var tableName = (string)row[0];
                    var indexName = (string)row[1];

                    var sql = "if exists(select * from sys.indexes where name = '" + indexName + "')\r\n" +
                                "ALTER INDEX [" + indexName + "] ON [" + tableName + "] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE);";
                    SqlHelper.ExecuteSql(connectionString, sql, null, false);
                    index++;
                    LoggerCQ.LogInfo("ApplyFix_CompressTimestamp: Index=" + indexName + ", Progress:" + index + "/" + count);
                }

            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }

        }

        public static void ApplyFix_XYIndexes(string connectionString)
        {
            try
            {
                using (var context = new DatastoreEntities(connectionString))
                {
                    var list = context.Repository.Where(x => x.ParentId == null).Select(x => x.UniqueKey).ToList();

                    //Create the tables
                    var index = 0;
                    foreach (var ID in list)
                    {
                        var sb = new StringBuilder();

                        //List Tables
                        {
                            var schema = RepositoryManager.GetSchema(ID);
                            foreach (var ditem in schema.DimensionList.Where(x => x.DimensionType == RepositorySchema.DimensionTypeConstants.List).ToList())
                            {
                                var tableName = SqlHelper.GetListTableName(ID, ditem.DIdx);
                                //Remove old index
                                var indexName = "PK_" + tableName;
                                sb.AppendLine("if exists(select * from sys.indexes where name = '" + indexName + "')");
                                sb.AppendLine("ALTER TABLE [" + tableName + "] DROP CONSTRAINT [" + indexName + "]");

                                //Create new index
                                sb.AppendLine("if not exists(select * from sys.indexes where name = '" + indexName + "')");
                                sb.AppendLine("ALTER TABLE [" + tableName + "] ADD CONSTRAINT [" + indexName + "] PRIMARY KEY CLUSTERED ([DVIdx], [" + SqlHelper.RecordIdxField + "]);");
                            }
                        }

                        //Dimension Table
                        {
                            var tableName = SqlHelper.GetDimensionValueTableName(ID);
                            //Remove old index
                            var indexName = "PK_" + tableName;
                            sb.AppendLine("if exists(select * from sys.indexes where name = '" + indexName + "')");
                            sb.AppendLine("ALTER TABLE [" + tableName + "] DROP CONSTRAINT [" + indexName + "]");

                            //Create new index
                            sb.AppendLine("if not exists(select * from sys.indexes where name = '" + indexName + "')");
                            sb.AppendLine("ALTER TABLE [" + tableName + "] ADD CONSTRAINT [" + indexName + "] PRIMARY KEY NONCLUSTERED ([DIdx],[DVIdx]);");
                        }

                        try
                        {
                            SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false, false);
                        }
                        catch (Exception ex)
                        {
                            LoggerCQ.LogError(ex);
                        }

                        index++;
                        LoggerCQ.LogInfo("ApplyFix_XYIndexes: Index=" + index + ", Progress:" + index + "/" + list.Count);
                    }
                }

            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void ApplyFix_AddYIndex2(string connectionString)
        {
            try
            {
                using (var context = new DatastoreEntities(connectionString))
                {
                    var list = context.Repository.Where(x => x.ParentId == null).Select(x => x.UniqueKey).ToList();

                    //Create the tables
                    var index = 0;
                    foreach (var ID in list)
                    {
                        var sb = new StringBuilder();

                        //List Tables
                        {
                            var schema = RepositoryManager.GetSchema(ID);
                            foreach (var ditem in schema.DimensionList.Where(x => x.DimensionType == RepositorySchema.DimensionTypeConstants.List).ToList())
                            {
                                var tableName = SqlHelper.GetListTableName(ID, ditem.DIdx);

                                //Create new index
                                var indexName = "IDX_" + tableName + "_DVIDX";
                                sb.AppendLine("if not exists(select * from sys.indexes where name = '" + indexName + "')");
                                sb.AppendLine("CREATE NONCLUSTERED INDEX [" + indexName + "] ON [" + tableName + "] ([DVIdx])");
                            }
                        }

                        try
                        {
                            SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false, false);
                        }
                        catch (Exception ex)
                        {
                            LoggerCQ.LogError(ex);
                        }

                        index++;
                        LoggerCQ.LogInfo("ApplyFix_AddYIndex2: Index=" + index + ", Progress:" + index + "/" + list.Count);
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void ApplyFix_ChangeYPKType(string connectionString)
        {
            try
            {
                using (var context = new DatastoreEntities(connectionString))
                {
                    var list = context.Repository.Where(x => x.ParentId == null).Select(x => x.UniqueKey).ToList();

                    //Create the tables
                    var index = 0;
                    foreach (var ID in list)
                    {
                        //List Tables
                        {
                            var schema = RepositoryManager.GetSchema(ID);
                            foreach (var ditem in schema.DimensionList.Where(x => x.DimensionType == RepositorySchema.DimensionTypeConstants.List).ToList())
                            {
                                var tableName = SqlHelper.GetListTableName(ID, ditem.DIdx);

                                //Find list tables where the __RecordIdx is NOT BIGINT
                                var sqlCheck = "select * from sys.columns c inner join sys.objects o on c.object_id = o.object_id where o.name = '" + tableName + "' and c.name = '" + SqlHelper.RecordIdxField + "' and c.system_type_id <> 127";
                                var ds = SqlHelper.GetDataset(connectionString, sqlCheck);
                                if (ds.Tables.Count == 1 && ds.Tables[0].Rows.Count == 1)
                                {
                                    var sb = new StringBuilder();

                                    //Drop PK index
                                    var pkIndexName = "PK_" + tableName;
                                    sb.AppendLine("if exists(select * from sys.indexes where name = '" + pkIndexName + "')");
                                    sb.AppendLine("alter table [" + tableName + "] drop CONSTRAINT [" + pkIndexName + "]");

                                    //Drop DVI index
                                    var dviIndexName = "IDX_" + tableName + "_DVIDX";
                                    sb.AppendLine("if exists(select * from sys.indexes where name = '" + dviIndexName + "')");
                                    sb.AppendLine("drop index [" + dviIndexName + "] ON [" + tableName + "]");

                                    //Drop __RecordIdx index
                                    var ridxIndexName = "IDX_" + tableName + "_RecordIdx";
                                    sb.AppendLine("if exists(select * from sys.indexes where name = '" + ridxIndexName + "')");
                                    sb.AppendLine("drop index [" + ridxIndexName + "] ON [" + tableName + "]");

                                    //Alter table to make __RecordIdx a BIGINT
                                    sb.AppendLine("alter table [" + tableName + "] alter column [" + SqlHelper.RecordIdxField + "] bigint not null");

                                    //Add back indexes
                                    sb.AppendLine("alter table [" + tableName + "] ADD CONSTRAINT [" + pkIndexName + "] PRIMARY KEY CLUSTERED ([" + SqlHelper.RecordIdxField + "], [DVIdx]);");
                                    sb.AppendLine("CREATE NONCLUSTERED INDEX [" + dviIndexName + "] ON [" + tableName + "] ([DVIdx]);");
                                    sb.AppendLine("CREATE NONCLUSTERED INDEX [" + ridxIndexName + "] ON [" + tableName + "] ([" + SqlHelper.RecordIdxField + "]);");

                                    try
                                    {
                                        SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false, false);
                                    }
                                    catch (Exception ex)
                                    {
                                        LoggerCQ.LogError(ex);
                                    }

                                }
                            }
                        }

                        index++;
                        LoggerCQ.LogInfo("ApplyFix_ChangeYPKType: Index=" + index + ", Progress=" + index + "/" + list.Count + ", ID=" + ID);
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static bool ApplyFix_ReorgFTS(string connectionString, RepositoryManager manager)
        {
            try
            {
                using (var context = new DatastoreEntities(connectionString))
                {
                    //No longer use a single catalog, so find all old indexes on that singular catalog and re-org
                    //Old catalog was named "DatastoreFTS", now every table has its own catalog
                    #region Get all tables that need re-organizing
                    var sb = new StringBuilder();
                    sb.AppendLine("SELECT ");
                    sb.AppendLine("    t.name AS TableName");
                    sb.AppendLine("FROM ");
                    sb.AppendLine("    sys.tables t ");
                    sb.AppendLine("INNER JOIN ");
                    sb.AppendLine("    sys.fulltext_indexes fi ");
                    sb.AppendLine("ON ");
                    sb.AppendLine("    t.[object_id] = fi.[object_id] ");
                    sb.AppendLine("INNER JOIN ");
                    sb.AppendLine("    sys.fulltext_catalogs c ");
                    sb.AppendLine("ON ");
                    sb.AppendLine("    fi.fulltext_catalog_id = c.fulltext_catalog_id");
                    sb.AppendLine("INNER JOIN ");
                    sb.AppendLine("    sys.indexes i");
                    sb.AppendLine("ON ");
                    sb.AppendLine("    fi.unique_index_id = i.index_id");
                    sb.AppendLine("    AND fi.[object_id] = i.[object_id]");
                    sb.AppendLine("	   AND (c.name = 'DatastoreFTS' OR fi.change_tracking_state <> 'A')");
                    var ds = SqlHelper.GetDataset(connectionString, sb.ToString());
                    var tableList = new List<string>();
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        tableList.Add((string)dr[0]);
                    }
                    #endregion

                    //If nothing to do, get out of here
                    if (!tableList.Any()) return true;

                    //This can be done async as it will take a while
                    var index = 0;
                    var task = Task.Factory.StartNew(() =>
                    {
                        //Let the system come online and we can process this async task afterwards
                        System.Threading.Thread.Sleep(30 * 1000);
                        foreach (var dataTable in tableList)
                        {
                            var timer = Stopwatch.StartNew();
                            var id = dataTable.Replace("Z_", string.Empty);
                            var schema = RepositoryManager.GetSchema(new Guid(id));
                            var sqlList = SqlHelper.GetSqlFTS(dataTable, schema);
                            using (var q = new AcquireWriterLock(new Guid(id), "FullTextSetup"))
                            {
                                foreach (var sql in sqlList)
                                    SqlHelper.ExecuteSql(connectionString, sql, null, false);
                            }
                            timer.Stop();
                            index++;
                            LoggerCQ.LogInfo("ApplyFix_ReorgFTS: ID=" + id + ", Elapsed=" + timer.ElapsedMilliseconds + ", Progress:" + index + "/" + tableList.Count);
                        }
                    });
                }
                return false;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return false;
            }
        }

        public static void ApplyFix_AddZHash(string connectionString)
        {
            try
            {
                var timer = Stopwatch.StartNew();
                var index = 0;
                using (var context = new DatastoreEntities(connectionString))
                {
                    var repositorylist = context.Repository.Select(x => x.UniqueKey).ToList();
                    foreach (var r in repositorylist)
                    {
                        var dataTable = SqlHelper.GetTableName(r);
                        var sb = new StringBuilder();
                        sb.AppendLine($"if exists(select * from sys.objects where name = '{dataTable}' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = '{SqlHelper.HashField}' and o.name = '{dataTable}')");
                        sb.AppendLine($"ALTER TABLE [{dataTable}] ADD [{SqlHelper.HashField}] [BIGINT] CONSTRAINT [DF__{dataTable}_{SqlHelper.HashField}] DEFAULT 0 NOT NULL");

                        var indexName = SqlHelper.GetIndexName(new FieldDefinition { Name = SqlHelper.HashField }, dataTable);
                        sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}')");
                        sb.AppendLine("BEGIN");
                        sb.AppendLine($"CREATE NONCLUSTERED INDEX [{indexName}] ON [{dataTable}] ([{SqlHelper.HashField}] ASC);");

                        if (ConfigHelper.SupportsCompression)
                            sb.AppendLine($"ALTER INDEX [{indexName}] ON [{dataTable}] REBUILD WITH (DATA_COMPRESSION = PAGE);");

                        sb.AppendLine("END");
                        sb.AppendLine();

                        try
                        {
                            SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false, false);
                            LoggerCQ.LogInfo($"ApplyFix_AddZHash: ID={r}, Elapsed={timer.ElapsedMilliseconds}, Progress:{index}/{repositorylist.Count}");
                        }
                        catch (Exception ex)
                        {
                            LoggerCQ.LogError(ex);
                        }
                        index++;
                    }
                }
                timer.Stop();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void ApplyFix_IndexOptimization(string connectionString)
        {
            try
            {
                var timer = Stopwatch.StartNew();
                var index = 0;
                using (var context = new DatastoreEntities(connectionString))
                {
                    var repositorylist = context.Repository.Select(x => x.UniqueKey).ToList();
                    foreach (var r in repositorylist)
                    {
                        var sb = new StringBuilder();
                        var schema = RepositoryManager.GetSchema(r);
                        var dataTable = SqlHelper.GetTableName(r);

                        //Hash Index
                        {
                            var indexName = SqlHelper.GetIndexName(new FieldDefinition { Name = SqlHelper.HashField }, dataTable);
                            sb.AppendLine("if exists(select * from sys.indexes where name = '" + indexName + "')");
                            sb.AppendLine("DROP INDEX [" + indexName + "] ON [" + dataTable + "]");
                            sb.AppendLine();
                        }

                        //Timestamp Index
                        {
                            var indexName = SqlHelper.GetIndexName(new FieldDefinition { Name = SqlHelper.TimestampField }, dataTable);
                            sb.AppendLine("if exists(select * from sys.indexes where name = '" + indexName + "')");
                            sb.AppendLine("DROP INDEX [" + indexName + "] ON [" + dataTable + "]");
                            sb.AppendLine();
                        }

                        //Drop all indexes for fields that are not supposed to have an index
                        foreach (var field in schema.FieldList.Where(x => !x.IsPrimaryKey &&
                                            x.DataType != RepositorySchema.DataTypeConstants.GeoCode &&
                                            x.DataType != RepositorySchema.DataTypeConstants.List))
                        {
                            if (!field.AllowIndex)
                            {
                                var indexName = SqlHelper.GetIndexName(field, dataTable);
                                sb.AppendLine("if exists(select * from sys.indexes where name = '" + indexName + "')");
                                sb.AppendLine("DROP INDEX [" + indexName + "] ON [" + dataTable + "]");
                                sb.AppendLine();
                            }
                        }

                        try
                        {
                            var c = SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false, false);
                            LoggerCQ.LogInfo("ApplyFix_IndexOptimization: ID=" + r + ", Elapsed=" + timer.ElapsedMilliseconds + ", Progress:" + index + "/" + repositorylist.Count);
                        }
                        catch (Exception ex)
                        {
                            LoggerCQ.LogError(ex);
                        }
                        index++;
                    }
                }
                timer.Stop();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static void ApplyFix_ListTableCK(string connectionString)
        {
            var timer = Stopwatch.StartNew();
            var processed = 0;
            try
            {
                var repoIndex = 0;
                using (var context = new DatastoreEntities(connectionString))
                {
                    var repositorylist = context.Repository
                        .OrderByDescending(x => x.ItemCount)
                        .Select(x => x.UniqueKey)
                        .ToList();

                    //Take bottom 5 and add to top so first 5 will be fast and we can verify
                    if (repositorylist.Count > 10)
                    {
                        repositorylist.Reverse();
                        var small5 = repositorylist.Take(5).ToList();
                        repositorylist.RemoveRange(0, 5);
                        repositorylist.AddRange(small5);
                        repositorylist.Reverse();
                    }

                    foreach (var r in repositorylist)
                    {
                        LoggerCQ.LogInfo($"ApplyFix_ListTableCK Before: ID={r}");

                        var schema = RepositoryManager.GetSchema(r);
                        var listDimensions = schema.DimensionList.Where(x => x.DimensionType == RepositorySchema.DimensionTypeConstants.List).ToList();

                        var timer2 = Stopwatch.StartNew();

                        //Step 1
                        foreach (var ld in listDimensions)
                        {
                            var sb = new StringBuilder();
                            var table = SqlHelper.GetListTableName(r, ld.DIdx);
                            var indexName = $"PK_{table}";

                            #region Delete Index if the [DVIdx] is second column. We are re-arrange to make it first column in clustered index
                            sb.AppendLine($"if exists(select * from sys.indexes i");
                            sb.AppendLine($"inner join sys.index_columns ic on i.object_id = ic.object_id and i.index_id = ic.index_id");
                            sb.AppendLine($"inner join sys.columns c on c.object_id = i.object_id and c.column_id = ic.column_id");
                            sb.AppendLine($"where i.name = '{indexName}' and c.name = 'DVIdx' and ic.key_ordinal = 2)");
                            sb.AppendLine("BEGIN");
                            sb.AppendLine($"ALTER TABLE [{table}] DROP CONSTRAINT [{indexName}]");
                            sb.AppendLine("END");
                            sb.AppendLine("GO");
                            #endregion

                            #region Add Index
                            sb.AppendLine($"if not exists(select * from sys.indexes i where i.name = '{indexName}') and exists (select * from sys.tables where name = '{table}')");
                            sb.AppendLine("BEGIN");
                            sb.AppendLine($"ALTER TABLE[dbo].[{table}] ");
                            sb.AppendLine($"ADD CONSTRAINT[{indexName}] PRIMARY KEY CLUSTERED");
                            sb.AppendLine($"([DVIdx] ASC, [{SqlHelper.RecordIdxField}] ASC)");
                            sb.AppendLine("END");
                            sb.AppendLine("GO");
                            #endregion

                            if (ConfigHelper.SupportsCompression)
                            {
                                sb.AppendLine($"if exists(select * from sys.indexes i where i.name = '{indexName}')");
                                sb.AppendLine($"ALTER INDEX [{indexName}] ON [{table}] REBUILD WITH (DATA_COMPRESSION = PAGE);");
                            }

                            try
                            {
                                var c = SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false, false);
                            }
                            catch (Exception ex)
                            {
                                LoggerCQ.LogError(ex);
                            }
                            processed++;
                        }

                        //Step 2
                        foreach (var ld in listDimensions)
                        {
                            var sb = new StringBuilder();
                            var table = SqlHelper.GetListTableName(r, ld.DIdx);

                            #region Delete Index
                            var indexName = $"IDX_{table}_DVIDX";
                            sb.AppendLine($"if exists(select * from sys.indexes where name = '{indexName}')");
                            sb.AppendLine($"DROP INDEX [{indexName}] ON [{table}]");
                            sb.AppendLine("GO");
                            #endregion

                            #region Delete Index
                            indexName = $"IDX_{table}_RecordIdx";
                            sb.AppendLine($"if exists(select * from sys.indexes where name = '{indexName}')");
                            sb.AppendLine($"DROP INDEX [{indexName}] ON [{table}]");
                            sb.AppendLine("GO");
                            #endregion

                            #region Add Covering Index
                            var fileGroup = RepositoryManager.GetRandomFileGroup();
                            indexName = $"NCK_{table}";
                            sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}') and exists (select * from sys.tables where name = '{table}')");
                            sb.AppendLine($"CREATE NONCLUSTERED INDEX [{indexName}] ON [{table}] ([{SqlHelper.RecordIdxField}], [DVIdx])");
                            if (!string.IsNullOrEmpty(fileGroup)) sb.AppendLine($"ON [{fileGroup}];");
                            sb.AppendLine("GO");
                            #endregion

                            if (ConfigHelper.SupportsCompression)
                            {
                                sb.AppendLine($"if exists(select * from sys.indexes where name = '{indexName}')");
                                sb.AppendLine($"ALTER INDEX [{indexName}] ON [{table}] REBUILD WITH (DATA_COMPRESSION = PAGE);");
                                sb.AppendLine("GO");
                            }

                            try
                            {
                                var c = SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false, false);
                            }
                            catch (Exception ex)
                            {
                                LoggerCQ.LogError(ex);
                            }
                            processed++;
                        }

                        //Step 3
                        {
                            var sb = new StringBuilder();
                            var userPermissionTableName = SqlHelper.GetUserPermissionTableName(schema.ID);
                            var fileGroup = RepositoryManager.GetRandomFileGroup();

                            //Delete Index
                            var indexName = $"PK_{userPermissionTableName}";
                            sb.AppendLine($"if exists(select * from sys.indexes where name = '{indexName}' and is_primary_key = 1)");
                            sb.AppendLine($"ALTER TABLE [{userPermissionTableName}] DROP CONSTRAINT [{indexName}];");
                            sb.AppendLine($"if exists(select * from sys.indexes where name = '{indexName}' and is_primary_key = 0)");
                            sb.AppendLine($"DROP INDEX [{indexName}] ON [{userPermissionTableName}]");
                            sb.AppendLine("GO");

                            //Create PK
                            sb.AppendLine($"if exists (select * from sys.tables where name = '{userPermissionTableName}')");
                            sb.AppendLine($"ALTER TABLE [{userPermissionTableName}] ADD CONSTRAINT [{indexName}] PRIMARY KEY CLUSTERED ([UserId], [FKField])");
                            if (!string.IsNullOrEmpty(fileGroup)) sb.AppendLine($"ON [{fileGroup}];");
                            else sb.AppendLine(";");
                            sb.AppendLine("GO");

                            //Add Covering Index
                            indexName = $"NCK_{userPermissionTableName}";
                            sb.AppendLine($"if not exists(select * from sys.indexes where name = '{indexName}') and exists (select * from sys.tables where name = '{userPermissionTableName}')");
                            sb.AppendLine($"CREATE NONCLUSTERED INDEX [{indexName}] ON [{userPermissionTableName}] ([FKField], [UserId])");
                            if (!string.IsNullOrEmpty(fileGroup)) sb.AppendLine($"ON [{fileGroup}];");
                            else sb.AppendLine(";");
                            sb.AppendLine("GO");

                            try
                            {
                                var c = SqlHelper.ExecuteSql(connectionString, sb.ToString(), null, false, false);
                            }
                            catch (Exception ex)
                            {
                                LoggerCQ.LogError(ex);
                            }
                            processed++;
                        }

                        timer2.Stop();
                        repoIndex++;
                        LoggerCQ.LogInfo($"ApplyFix_ListTableCK: ID={r}, Elapsed={timer2.ElapsedMilliseconds}, Count={listDimensions.Count}, Progress:{repoIndex}/{repositorylist.Count}");
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
                LoggerCQ.LogInfo($"ApplyFix_ListTableCK: Processed={processed}, Elapsed={timer.ElapsedMilliseconds}");
            }
        }

    }
}
