using System;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.ServiceProcess;
using System.Threading.Tasks;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.Install;
using Gravitybox.Datastore.Server.Core;
using StackExchange.Exceptional;

namespace Gravitybox.Datastore.WinService
{
    public partial class PersistentService : ServiceBase
    {
        #region Class Members

        private const int ThrottleMax = 1000;
        private static Gravitybox.Datastore.Common.ISystemCore _core = null;
        private bool _enableHouseKeeping = true;

        #endregion

        #region Constructor

        public PersistentService()
        {
            InitializeComponent();

            var serviceName = ConfigurationManager.AppSettings["serviceName"];

            if (!string.IsNullOrWhiteSpace(serviceName))
            {
                this.ServiceName = serviceName;
            }
        }

        public PersistentService(bool enableHouseKeeping)
        {
            _enableHouseKeeping = enableHouseKeeping;
        }

        #endregion

        #region Service Events

        protected override void OnStart(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += domainExceptionsHandler;
            TaskScheduler.UnobservedTaskException += taskExceptionsHandler;
            this.Start();
        }

        protected override void OnStop()
        {
            //KillTimer();
            try
            {
                ConfigHelper.ShutDown();
                if (_core != null) _core.ShutDown();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError("Error 0x2400: Shutdown Failed");
            }

            LoggerCQ.LogInfo("Services Stopped");
        }

        protected override void OnShutdown()
        {
            try
            {
                base.OnShutdown();
                _core.ShutDown();
                LoggerCQ.LogInfo("Services ShutDown");
                //KillTimer();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError("Error 0x2401: Shutdown Failed");
                throw;
            }
        }

        #endregion

        #region Methods

        public void Cleanup()
        {
            try
            {
                _core.ShutDown();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public void Start()
        {
            try
            {
                //Do this to avoid an infinite hang if the firewall has blocked the port
                //You cannot shut down the service if blocked because it never finishes startup
                var t = new System.Threading.Thread(StartupEndpoint);
                t.Start();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        private void StartupEndpoint()
        {
            var config = new SetupConfig();
            try
            {
                LoggerCQ.LogInfo("Attempting to upgrade database.");
                var connectionStringSettings = ConfigurationManager.ConnectionStrings["DatastoreEntities"];
                var connectionStringBuilder = new SqlConnectionStringBuilder(connectionStringSettings.ConnectionString)
                {
                    InitialCatalog = "Master"
                };

                //Make sure there are no other nHydrate installations on this database
                if (DbMaintenanceHelper.ContainsOtherInstalls(connectionStringSettings.ConnectionString))
                {
                    LoggerCQ.LogError($"The database contains another installation. This is an error condition. Database={connectionStringBuilder.InitialCatalog}");
                    throw new Exception($"The database contains another installation. This is an error condition. Database={connectionStringBuilder.InitialCatalog}");
                }

                //Even a blank database gets updated below so save if DB is blank when started
                var isBlank = DbMaintenanceHelper.IsBlank(connectionStringSettings.ConnectionString);

                var installer = new DatabaseInstaller();
                if (installer.NeedsUpdate(connectionStringSettings.ConnectionString))
                {
                    var setup = new InstallSetup
                    {
                        AcceptVersionWarningsChangedScripts = true,
                        AcceptVersionWarningsNewScripts = true,
                        ConnectionString = connectionStringSettings.ConnectionString,
                        InstallStatus = InstallStatusConstants.Upgrade,
                        MasterConnectionString = connectionStringBuilder.ToString(),
                        SuppressUI = true,
                    };
                    installer.Install(setup);
                }

                //If new database then add file split data files to reduce file locking
                if (isBlank)
                {
                    try
                    {
                        DbMaintenanceHelper.SplitDbFiles(connectionStringSettings.ConnectionString);
                        LoggerCQ.LogInfo("New database has split data files.");
                    }
                    catch
                    {
                        LoggerCQ.LogWarning("New database could not split data files.");
                    }

                    try
                    {
                        var configFile = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location), "setup.config");
                        if (File.Exists(configFile))
                        {
                            var barr = File.ReadAllBytes(configFile);
                            config = ServerUtilities.DeserializeObject<SetupConfig>(barr);
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new Exception($"Setup configuration file is not valid.");
                    }

                    if (config != null)
                    {
                        if (!string.IsNullOrEmpty(config.ListDataPath) && !Directory.Exists(config.ListDataPath))
                            throw new Exception("The setup configuration file value 'ListDataPath' is not valid");
                        if (!string.IsNullOrEmpty(config.IndexPath) && !Directory.Exists(config.IndexPath))
                            throw new Exception("The setup configuration file value 'IndexPath' is not valid");

                        //Create a file group for List tables
                        config.ListDataPath = DbMaintenanceHelper.CreateFileGroup(connectionStringSettings.ConnectionString, config.ListDataPath, SetupConfig.YFileGroup);

                        //Create a file group for Indexes
                        config.IndexPath = DbMaintenanceHelper.CreateFileGroup(connectionStringSettings.ConnectionString, config.IndexPath, SetupConfig.IndexFileGroup);
                    }
                }

            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, "Failed on database upgrade.");
                throw new Exception("Failed on database upgrade.");
            }

            LoggerCQ.LogInfo("Service started begin");
            try
            {
                #region Primary Endpoint

                var service = new Gravitybox.Datastore.Server.Core.SystemCore(ConfigurationManager.ConnectionStrings["DatastoreEntities"].ConnectionString, _enableHouseKeeping);
                if (config != null)
                    ConfigHelper.SetupConfig = config;

                #region Determine if configured port is free
                var isPortFree = false;
                do
                {
                    try
                    {
                        //Determine if can connect to port
                        using (var p1 = new System.Net.Sockets.TcpClient("localhost", ConfigHelper.Port))
                        {
                        }
                        //If did connect successfully then there is already something on this port
                        isPortFree = false;
                        LoggerCQ.LogInfo($"Port {ConfigHelper.Port} is in use...");
                        System.Threading.Thread.Sleep(3000); //wait...
                    }
                    catch (Exception ex)
                    {
                        //If there is an error connecting then nothing is listening on that port so FREE
                        isPortFree = true;
                    }
                } while (!isPortFree);
                #endregion

                var primaryAddress = new Uri($"net.tcp://localhost:{ConfigHelper.Port}/__datastore_core");
                var primaryHost = new ServiceHost(service, primaryAddress);

                //Initialize the service
                var netTcpBinding = new NetTcpBinding();
                netTcpBinding.MaxConnections = ThrottleMax;
                netTcpBinding.Security.Mode = SecurityMode.None;
                primaryHost.AddServiceEndpoint(typeof(Gravitybox.Datastore.Common.ISystemCore), netTcpBinding, string.Empty);

                //Add more threads
                var stb = new ServiceThrottlingBehavior
                {
                    MaxConcurrentSessions = ThrottleMax,
                    MaxConcurrentCalls = ThrottleMax,
                    MaxConcurrentInstances = ThrottleMax,
                };
                primaryHost.Description.Behaviors.Add(stb);

                primaryHost.Open();

                //Create Core Listener
                var primaryEndpoint = new EndpointAddress(primaryHost.BaseAddresses.First().AbsoluteUri);
                var primaryClient = new ChannelFactory<Gravitybox.Datastore.Common.ISystemCore>(netTcpBinding, primaryEndpoint);
                _core = primaryClient.CreateChannel();
                (_core as IContextChannel).OperationTimeout = new TimeSpan(0, 0, 120); //Timeout=2m

                #endregion

                LoadEngine(service);
                service.Manager.ResetMaster();
                LoggerCQ.LogInfo("Service started complete");
                ConfigHelper.StartUp();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        private static void LoadEngine(Gravitybox.Datastore.Server.Core.SystemCore core)
        {
            //Load Server Object
            var baseAddress = new Uri($"net.tcp://localhost:{ConfigHelper.Port}/__datastore_engine");
            var serviceInstance = core.Manager;
            var host = new ServiceHost(serviceInstance, baseAddress);

            //Initialize the service
            var myBinding = new NetTcpBinding()
            {
                MaxBufferSize = int.MaxValue,
                MaxReceivedMessageSize = int.MaxValue,
                MaxBufferPoolSize = 0,
                MaxConnections = ThrottleMax,
                ReaderQuotas = new System.Xml.XmlDictionaryReaderQuotas()
                {
                    MaxArrayLength = int.MaxValue,
                    MaxBytesPerRead = int.MaxValue,
                    MaxDepth = int.MaxValue,
                    MaxNameTableCharCount = int.MaxValue,
                    MaxStringContentLength = int.MaxValue,
                },
                OpenTimeout = new TimeSpan(0, 0, 10),
                ReceiveTimeout = new TimeSpan(0, 5, 0),
                CloseTimeout = new TimeSpan(0, 0, 120),
            };
            myBinding.Security.Mode = SecurityMode.None;
            var endpoint = host.AddServiceEndpoint(typeof(Gravitybox.Datastore.Common.IDataModel), myBinding, host.BaseAddresses.First().AbsoluteUri);

            foreach (var op in endpoint.Contract.Operations)
            {
                var dataContractBehavior = op.Behaviors.Find<DataContractSerializerOperationBehavior>();
                if (dataContractBehavior != null)
                {
                    dataContractBehavior.MaxItemsInObjectGraph = int.MaxValue;
                }
            }

            //var behavior = host.Description.Behaviors.Find<ServiceDebugBehavior>();
            //behavior.IncludeExceptionDetailInFaults = true;
            var behavior = host.Description.Behaviors.FirstOrDefault(x => x is System.ServiceModel.ServiceBehaviorAttribute);
            if (behavior != null)
                ((System.ServiceModel.ServiceBehaviorAttribute)behavior).IncludeExceptionDetailInFaults = true;

            //Add more threads
            var stb = new ServiceThrottlingBehavior
            {
                MaxConcurrentSessions = ThrottleMax,
                MaxConcurrentCalls = ThrottleMax,
                MaxConcurrentInstances = ThrottleMax,
            };
            host.Description.Behaviors.Add(stb);

            host.Open();
        }

        #endregion

        private static readonly UnhandledExceptionEventHandler domainExceptionsHandler = (s, args) =>
        {
            if (args.ExceptionObject is Exception e)
            {
                LoggerCQ.LogError(e);
                ErrorStore.LogExceptionWithoutContext(e);
            }
        };

        private static readonly EventHandler<UnobservedTaskExceptionEventArgs> taskExceptionsHandler = (s, args) =>
        {
            foreach (var ex in args.Exception.InnerExceptions)
            {
                ErrorStore.LogExceptionWithoutContext(ex, rollupPerServer: true);
            }
            args.SetObserved();
        };
    }
}
