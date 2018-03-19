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
                        var newfileName = Path.Combine(fi.DirectoryName, $"{builder.InitialCatalog}{ii}.ndf");
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
    }
}
