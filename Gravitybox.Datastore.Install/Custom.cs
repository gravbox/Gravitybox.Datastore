using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Install
{
    public static class DbMaintenanceHelper
    {
        public static bool ContainsOtherInstalls(string connectionString)
        {
            try
            {
                using (var conn = new System.Data.SqlClient.SqlConnection())
                {
                    conn.ConnectionString = connectionString;
                    conn.Open();

                    var da = new SqlDataAdapter("select * from sys.tables where name = '__nhydrateschema'", conn);
                    var ds = new DataSet();
                    da.Fill(ds);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        da = new SqlDataAdapter("SELECT [ModelKey] FROM __nhydrateschema", conn);
                        ds = new DataSet();
                        da.Fill(ds);
                        var t = ds.Tables[0];
                        var allKeys = new List<Guid>();
                        if (t.Rows.Count > 0)
                        {
                            allKeys.Add((Guid)t.Rows[0]["ModelKey"]);
                        }
                        allKeys.RemoveAll(x => x == new Guid(UpgradeInstaller.MODELKEY));
                        return allKeys.Any();
                    }
                }
                return false;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public static bool IsBlank(string connectionString)
        {
            try
            {
                using (var conn = new System.Data.SqlClient.SqlConnection())
                {
                    conn.ConnectionString = connectionString;
                    conn.Open();

                    var da = new SqlDataAdapter("select top 1 * from sys.tables", conn);
                    var ds = new DataSet();
                    da.Fill(ds);
                    return ds.Tables[0].Rows.Count == 0;
                }
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        private static bool AllowFileAccess(string connectionString)
        {
            try
            {
                using (var conn = new System.Data.SqlClient.SqlConnection())
                {
                    conn.ConnectionString = connectionString;
                    conn.Open();

                    var da = new SqlDataAdapter("select @@version", conn);
                    var ds = new DataSet();
                    da.Fill(ds);
                    if (ds.Tables[0].Rows.Count != 1) return false;
                    var text = ds.Tables[0].Rows[0][0].ToString() + string.Empty;
                    return !text.Contains("Azure");
                }
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        private static string GetDataFilePath(string connectionString)
        {
            using (var conn = new System.Data.SqlClient.SqlConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                var da = new SqlDataAdapter("select * from sys.master_files where database_id = DB_ID() and type = 0", conn);
                var ds = new DataSet();
                da.Fill(ds);
                if (ds.Tables[0].Rows.Count == 0) return null;
                var fileName = ds.Tables[0].Rows[0]["physical_name"].ToString();
                var fi = new FileInfo(fileName);
                return fi.DirectoryName;
            }
        }

        public static void SplitDbFiles(string connectionString)
        {
            try
            {
                if (!AllowFileAccess(connectionString)) return;
                using (var conn = new System.Data.SqlClient.SqlConnection())
                {
                    conn.ConnectionString = connectionString;
                    conn.Open();

                    var da = new SqlDataAdapter("select * from sys.master_files where database_id = DB_ID() and type = 0", conn);
                    var ds = new DataSet();
                    da.Fill(ds);
                    if (ds.Tables[0].Rows.Count != 1) return;
                    var fileID = ds.Tables[0].Rows[0]["name"].ToString();
                    var fileName = ds.Tables[0].Rows[0]["physical_name"].ToString();
                    var fi = new FileInfo(fileName);

                    //Create N more data files
                    var builder = new SqlConnectionStringBuilder(connectionString);
                    for (var ii = 2; ii <= 4; ii++)
                    {
                        var newfileName = Path.Combine(fi.DirectoryName, $"{builder.InitialCatalog}_Data{ii}.ndf");
                        using (var command = new SqlCommand($"ALTER DATABASE {builder.InitialCatalog} ADD FILE (NAME = data{ii}, FILENAME = '{newfileName}', SIZE = 64MB, FILEGROWTH = 64MB)", conn))
                        {
                            SqlServers.ExecuteCommand(command);
                        }
                    }

                    //Set original file to grow at same rate
                    using (var command = new SqlCommand($"ALTER DATABASE {builder.InitialCatalog} MODIFY FILE (NAME = {fileID}, FILEGROWTH = 64MB)", conn))
                    {
                        SqlServers.ExecuteCommand(command);
                    }

                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public static string CreateFileGroup(string connectionString, string path, string groupName)
        {
            try
            {
                if (!AllowFileAccess(connectionString)) return null;
                using (var conn = new System.Data.SqlClient.SqlConnection())
                {
                    conn.ConnectionString = connectionString;
                    conn.Open();

                    //Check and create the file group
                    var da = new SqlDataAdapter($"select * from sys.filegroups where name = '{groupName}'", conn);
                    var ds = new DataSet();
                    da.Fill(ds);
                    if (ds.Tables[0].Rows.Count > 0) return null; //already exists

                    da = new SqlDataAdapter("select * from sys.master_files where database_id = DB_ID() and type = 0", conn);
                    ds = new DataSet();
                    da.Fill(ds);
                    var fileID = ds.Tables[0].Rows[0]["name"].ToString();

                    var builder = new SqlConnectionStringBuilder(connectionString);

                    //Create the new file group
                    using (var command = new SqlCommand($"ALTER DATABASE {builder.InitialCatalog} ADD FILEGROUP {groupName}", conn))
                    {
                        SqlServers.ExecuteCommand(command);
                    }

                    //If no path specified then just add to the data path
                    if (string.IsNullOrEmpty(path))
                    {
                        path = GetDataFilePath(connectionString);
                    }

                    //Create N "groupName" files in new file group
                    for (var ii = 1; ii <= 4; ii++)
                    {
                        var newfileName = Path.Combine(path, $"{builder.InitialCatalog}_{groupName}{ii}.ndf");
                        using (var command = new SqlCommand($"ALTER DATABASE {builder.InitialCatalog} ADD FILE (NAME = {groupName}{ii}, FILENAME = '{newfileName}', SIZE = 64MB, FILEGROWTH = 64MB) TO FILEGROUP {groupName}", conn))
                        {
                            SqlServers.ExecuteCommand(command);
                        }
                    }

                    return path;

                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }
    }
}
