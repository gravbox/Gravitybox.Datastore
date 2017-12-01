#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;

namespace Gravitybox.Datastore.Configuration
{
    internal static class DataHelper
    {
        public static List<string> GetFileGroups(string connectionString)
        {
            try
            {
                var sql = "select name from sys.filegroups where (name <> 'PRIMARY') and type_desc <> 'MEMORY_OPTIMIZED_DATA_FILEGROUP'";
                var ds = GetDataset(connectionString, sql, new List<SqlParameter>());
                return (from DataRow dr in ds.Tables[0].Rows select (string) dr[0]).ToList();
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public static void CreateNewFileGroup(string connectionString, string path, int growth)
        {
            try
            {
                var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
                var list = GetFileGroups(connectionString);
                var index = 1;
                var newName = "CQGroup" + index;
                while (list.Contains(newName))
                {
                    index++;
                    newName = "CQGroup" + index;
                }

                var fileName = System.IO.Path.Combine(path, newName + "_FILE.ndf");

                var sb = new StringBuilder();
                sb.AppendLine("ALTER DATABASE [" + connectionStringBuilder.InitialCatalog + "] ADD filegroup [" + newName + "];");
                sb.AppendLine("ALTER DATABASE [" + connectionStringBuilder.InitialCatalog + "]");
                sb.AppendLine("  ADD FILE");
                sb.AppendLine("  (name = '" + newName + "_FILE',");
                sb.AppendLine("   filename = '" + fileName + "',");
                sb.AppendLine("   size = " + growth + "MB,");
                sb.AppendLine("   maxsize = unlimited,");
                sb.AppendLine("   filegrowth = " + growth + "MB)");
                sb.AppendLine("TO filegroup " + newName + ";");
                ExecuteSql(connectionString, sb.ToString());
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        #region ExecuteSql

        private static void ExecuteSql(string connectionString, string sql)
        {
            ExecuteSql(connectionString, sql, new List<SqlParameter>());
        }

        private static void ExecuteSql(string connectionString, string sql, List<SqlParameter> parameters)
        {
            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandTimeout = 600;
                        command.CommandText = sql;
                        command.CommandType = CommandType.Text;
                        command.Parameters.AddRange(parameters.ToArray());
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                //Logger.LogError(ex);
                throw;
            }
        }

        private static DataSet GetDataset(string connectionString, string sql, List<SqlParameter> parameters)
        {
            const int MAX_TRY = 5;
            var tryCount = 0;

            using (var command = new SqlCommand())
            {
                //Declare outside of try/catch so parameters are only added once to a collection (causes an error if loop more than once)
                command.CommandText = sql;
                command.CommandType = CommandType.Text;
                command.Parameters.AddRange(parameters.ToArray());

                do
                {
                    try
                    {
                        using (var connection = new SqlConnection(connectionString))
                        {
                            connection.Open();

                            command.Connection = connection;
                            var da = new SqlDataAdapter();
                            da.SelectCommand = command;
                            var ds = new DataSet();
                            da.Fill(ds);
                            return ds;
                        }
                    }
                    catch (Exception ex)
                    {
                        if (tryCount < MAX_TRY)
                        {
                            Thread.Sleep(100);
                            tryCount++;
                        }
                        else
                        {
                            //Logger.LogError(ex);
                            throw;
                        }
                    }
                } while (true);
            }
        }

        #endregion
    }
}