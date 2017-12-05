using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using Gravitybox.Datastore.Install;

namespace Gravitybox.Datastore.Configuration
{
    internal static class Program
    {
        /// <summary>
        ///     The main entry point for the application.
        /// </summary>
        [STAThread]
        private static void Main(string[] args)
        {
            //Application.EnableVisualStyles();
            //Application.SetCompatibleTextRenderingDefault(false);
            //Application.Run(new MainForm());

            /*
            FEATURES:
                Create database
                /createdatabase /connectionstring:"server=.\SQL2014;Integrated Security=SSPI;" /dbname:"HPDatastoreLTest" /datapath:"e:\database" /logpath:"e:\database" /growth:50
                /createdatabase /connectionstring:"server=.;Integrated Security=SSPI;" /dbname:"HPDatastore" /datapath:"c:\path" /logpath:"c:\logpath" /growth:100 /memopt
                /createdatabase /connectionstring:"server=.;Integrated Security=SSPI;" /dbname:"HPDatastore" /datapath:"C:\Databases" /logpath:"C:\Databases" /growth:50
                /createdatabase /connectionstring:"server=.;Integrated Security=SSPI;" /dbname:"HPDatastore" /datapath:"D:\Database" /logpath:"D:\Database" /growth:50
                /createdatabase /connectionstring:"server=.;Integrated Security=SSPI;" /dbname:"HPDatastore" /datapath:"F:\datastoreDB\data" /logpath:"F:\datastoreDB\log" /growth:100
                /createdatabase /connectionstring:"server=16.113.65.71\SQL2014;user id=sa;password=zxzxasas;" /dbname:"HPDatastore" /datapath:"Z:\Database" /logpath:"Z:\Database" /growth:100 /memopt
                /createdatabase /connectionstring:"server=.\SQL2014;Integrated Security=SSPI;" /dbname:"HPDatastore" /datapath:"D:\Database" /logpath:"D:\Database" /growth:20
                /createdatabase /connectionstring:"server=.\SQL2014;user id=sa;password=zxzxasas;" /dbname:"HPDatastoreZ" /datapath:"E:\Database" /logpath:"E:\Database" /growth:100
                /createdatabase /connectionstring:"server=16.113.65.71;user id=sa;password=Spi!pass007^;" /dbname:"HPDatastore2" /datapath:"F:\Database" /logpath:"F:\Database" /growth:100
                /updatedatabase /connectionstring:"server=.\SQL2014;initial catalog=AAAA;Integrated Security=SSPI;"
                /createdatabase /connectionstring:"server=.\SQL2014;Integrated Security=SSPI;" /dbname:"TestDatastore8" /datapath:"F:\Database" /logpath:"F:\Database" /growth:50

            Get filegroups
                /getfilegroups /connectionstring:"server=.;Integrated Security=SSPI;
                /getfilegroups /connectionstring:"server=.\SQL2014;Integrated Security=SSPI;

            Add filegroup
               /createfilegroup /connectionstring:"server=.;Integrated Security=SSPI;" /growth:100
               /createfilegroup /connectionstring:"server=.\SQL2014;Integrated Security=SSPI;" /growth:20 /datapath:"D:\Database"
               /createfilegroup /connectionstring:"server=16.113.65.71\SQL2014;user id=sa;password=zxzxasas" /growth:100 /datapath:"Z:\Database" /dbname:"HPDatastore"
            */

            foreach (var x in args)
            {
                Console.WriteLine(x);
            }

            //Create database
            if (args.Any(x => x == "/createdatabase"))
            {
                var connectionString = GetParamValue(args, "connectionstring");
                var masterConnectionString = connectionString;
                var databaseName = GetParamValue(args, "dbname");
                if (string.IsNullOrEmpty(databaseName)) databaseName = "HPDatastore";
                var datapath = GetParamValue(args, "datapath");
                var logpath = GetParamValue(args, "logpath");
                var growth = GetParamValue(args, "growth", 50);
                var memopt = args.Any(x => x == "/memopt");

                {
                    var builder = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "Master" };
                    connectionString = builder.ToString();
                }

                CreateDatabase(connectionString, databaseName, datapath, logpath, growth);
                connectionString = DoHousekeeping(connectionString, databaseName);
                RunInstaller(connectionString, masterConnectionString, databaseName);
                if (memopt)
                    CreateMemOpt(connectionString, datapath, databaseName);
            }
            else if (args.Any(x => x == "/updatedatabase"))
            {
                var connectionString = GetParamValue(args, "connectionstring");
                var masterConnectionString = connectionString;

                var builder = new SqlConnectionStringBuilder(connectionString);
                connectionString = DoHousekeeping(connectionString, builder.InitialCatalog);
                RunInstaller(connectionString, masterConnectionString, builder.InitialCatalog);
            }

            //List filegroups
            if (args.Any(x => x == "/getfilegroups"))
            {
                var connectionString = GetParamValue(args, "connectionstring");
                var b = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "HPDatastore" };
                connectionString = b.ToString();
                var l = DataHelper.GetFileGroups(connectionString);
                Console.WriteLine(@"FILEGROUPS (" + l.Count + @"):");
                l.ForEach(Console.WriteLine);
                Console.WriteLine(string.Empty);
            }

            //Add filegroup
            if (args.Any(x => x == "/createfilegroup"))
            {
                var connectionString = GetParamValue(args, "connectionstring");
                var databaseName = GetParamValue(args, "dbname");
                if (string.IsNullOrEmpty(databaseName)) databaseName = "HPDatastore";
                var growth = GetParamValue(args, "growth", 50);
                var datapath = GetParamValue(args, "datapath");

                if (growth < 1 || growth > 1024)
                    throw new Exception("Growth must be between 1MB and 1024MB.");

                var cs = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = databaseName };
                cs.ConnectTimeout = 60;
                DataHelper.CreateNewFileGroup(cs.ToString(), datapath, growth);
            }

            if (!args.Any(x => x == "/quiet" || x == "/q"))
            {
                Console.WriteLine();
                Console.WriteLine(@"Press <ENTER> to end...");
                Console.ReadLine();
            }
        }

        private static void CreateMemOpt(string connectionString, string datapath, string databaseName)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var sb = new StringBuilder();
                var queryList = new List<String>();
                {
                    sb.AppendLine("if not exists(select * From sys.filegroups where name = 'MemCache')");
                    sb.AppendLine("BEGIN");
                    sb.AppendLine("ALTER DATABASE [" + databaseName + "] ADD FILEGROUP MemCache CONTAINS MEMORY_OPTIMIZED_DATA");
                    sb.AppendLine("ALTER DATABASE [" + databaseName + "] ADD FILE (name='MemCache', filename='@datapath\\mem_" + GetHash(databaseName) + "') TO FILEGROUP MemCache");
                    sb.AppendLine("ALTER DATABASE [" + databaseName + "] SET MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT=ON");
                    sb.AppendLine("if exists (select * from [ConfigurationSetting] where Name = 'MemOpt')");
                    sb.AppendLine("update [ConfigurationSetting] set Value = 'true' where Name = 'MemOpt'");
                    sb.AppendLine("else");
                    sb.AppendLine("insert into [ConfigurationSetting] (Name, Value) values ('MemOpt', 'true')");
                    sb.AppendLine("END");
                    queryList.Add(sb.ToString().Replace("@datapath", datapath));
                }

                try
                {
                    foreach (var sql in queryList)
                    {
                        using (var command = connection.CreateCommand())
                        {
                            command.CommandTimeout = 600;
                            command.CommandText = sql;
                            command.CommandType = CommandType.Text;
                            command.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception)
                {
                    Console.Write("Failed to create in memory tables");
                    throw;
                }

                //Create the in memory temp table type
                try
                {
                    sb = new StringBuilder();
                    sb.AppendLine("if not exists(select * from sys.types where name = 'RIdxItem')");
                    sb.AppendLine("CREATE TYPE [RIdxItem] AS TABLE  ([__RecordIdx] bigint NOT NULL INDEX [RIdxItem_PK]) WITH (MEMORY_OPTIMIZED = ON);");
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandTimeout = 30;
                        command.CommandText = sb.ToString();
                        command.CommandType = CommandType.Text;
                        command.ExecuteNonQuery();
                    }
                }
                catch (Exception)
                {
                    Console.Write("Failed to create in memory tables");
                    throw;
                }

            }
        }

        private static void RunInstaller(string connectionString, string masterConnectionString, string databaseName)
        {
            var setup = new InstallSetup
            {
                AcceptVersionWarningsChangedScripts = true,
                AcceptVersionWarningsNewScripts = true,
                ConnectionString = connectionString,
                InstallStatus = InstallStatusConstants.Upgrade,
                MasterConnectionString = masterConnectionString,
                NewDatabaseName = databaseName
            };

            var installer = new DatabaseInstaller();
            installer.Install(setup);
        }

        private static string DoHousekeeping(string connectionString, string databaseName)
        {
            {
                var builder = new SqlConnectionStringBuilder(connectionString);
                builder.InitialCatalog = databaseName;
                connectionString = builder.ToString();
            }

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var sb = new StringBuilder();
                sb.AppendLine("ALTER DATABASE @dbName SET RECOVERY SIMPLE;");
                sb.AppendLine("if not exists(SELECT fulltext_catalog_id, name FROM sys.fulltext_catalogs where name = 'DatastoreFTS')");
                sb.AppendLine("CREATE FULLTEXT CATALOG [DatastoreFTS] ON FILEGROUP [PRIMARY] WITH ACCENT_SENSITIVITY = OFF;");

                using (var command = connection.CreateCommand())
                {
                    command.CommandTimeout = 600;
                    command.CommandText = sb.ToString().Replace("@dbName", databaseName);
                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
            }
            return connectionString;
        }

        private static void CreateDatabase(string connectionString, string databaseName, string datapath, string logpath, int growth)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var sb = new StringBuilder();
                sb.AppendLine("CREATE DATABASE @dbName");
                sb.AppendLine("ON");
                sb.AppendLine("( NAME = @dbName,");
                sb.AppendLine("   FILENAME = '@datapath\\@dbName.mdf',");
                sb.AppendLine("   FILEGROWTH = @growthMB )");
                sb.AppendLine("LOG ON");
                sb.AppendLine("( NAME = @dbName_log,");
                sb.AppendLine("    FILENAME = '@logpath\\@dbName.ldf',");
                sb.AppendLine("   FILEGROWTH = @growthMB ) ;");

                var commandText = sb.ToString();

                commandText = commandText.Replace("@dbName", databaseName);
                commandText = commandText.Replace("@datapath", datapath);
                commandText = commandText.Replace("@logpath", logpath);
                commandText = commandText.Replace("@growth", growth.ToString(CultureInfo.InvariantCulture));

                using (var command = connection.CreateCommand())
                {
                    command.CommandTimeout = 600;
                    command.CommandText = commandText;
                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
            }
        }

        private static string GetParamValue(IEnumerable<string> args, string key)
        {
            var retval = string.Empty;
            var v = args.FirstOrDefault(x => x.StartsWith("/" + key + ":"));
            if (!string.IsNullOrEmpty(v))
            {
                var delimiterIndex = v.IndexOf(':');

                if (delimiterIndex >= 0)
                {
                    retval = v.Substring(delimiterIndex + 1);
                }
                if (retval.StartsWith("\"")) retval = retval.Substring(1, retval.Length - 1);
                if (retval.EndsWith("\"")) retval = retval.Substring(0, retval.Length - 1);
            }
            return retval;
        }

        private static int GetParamValue(IEnumerable<string> args, string key, int defaultValue)
        {
            var s = GetParamValue(args, key);
            int v;
            return int.TryParse(s, out v) ? v : defaultValue;
        }

        public static string GetHash(string v)
        {
            const string salt = "atl@!xmf!$Jhg";
            var arrInput = System.Security.Cryptography.SHA1CryptoServiceProvider.Create().ComputeHash(System.Text.Encoding.ASCII.GetBytes(salt + v));
            int i;
            var sOutput = new StringBuilder(arrInput.Length);
            for (i = 0; i < arrInput.Length; i++)
            {
                sOutput.Append(arrInput[i].ToString("X2"));
            }
            return sOutput.ToString();
        }

    }
}