using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.ServiceProcess;
using System.Threading;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.Install;
using Gravitybox.Datastore.Server.Core;

namespace Gravitybox.Datastore.WinService
{
    internal static class Program
    {
        private static void Main(string[] args)
        {
            LoggerCQ.LogInfo("Initializing Service...");

#if DEBUG
            LoggerCQ.LogInfo("(Debug Build)");
#endif

            //Try to connect to database and if successfull the assume service will start
            try
            {
                var installer = new DatabaseInstaller();
                var connectionStringSettings = ConfigurationManager.ConnectionStrings["DatastoreEntities"];

                //Just wait a few seconds to determine if the database is there
                var cb = new System.Data.SqlClient.SqlConnectionStringBuilder(connectionStringSettings.ConnectionString);
                cb.ConnectTimeout = 12;

                var b = installer.NeedsUpdate(cb.ToString());
            }
            catch(Exception ex)
            {
                LoggerCQ.LogError(ex, "Failed to connect to database.");
                throw new Exception("Failed to connect to database.");
            }

            LoggerCQ.LogInfo("Database connection verified.");

            if (args.Any(x => x == "-console" || x == "/console"))
            {
                try
                {
                    var service = new PersistentService(args.Any(x => x == "-echo" || x == "/echo"));
                    service.Start();
                    Console.WriteLine("Press <ENTER> to stop...");
                    Console.ReadLine();
                    service.Cleanup();
                    service.Stop();
                }
                catch (Exception ex)
                {
                    LoggerCQ.LogError(ex, "Failed to start service from console.");
                    throw;
                }
            }
            else
            {
                try
                {
                    var servicesToRun = new ServiceBase[]
                                        {
                                            new PersistentService()
                                        };
                    ServiceBase.Run(servicesToRun);
                }
                catch (Exception ex)
                {
                    LoggerCQ.LogError(ex, "Failed to start service.");
                }
            }
        }
    }
}