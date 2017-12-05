using System;
using System.Linq;
using System.ServiceProcess;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.Configuration;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.Install;
using System.Data.SqlClient;
using Gravitybox.Datastore.Server.Core;

namespace Gravitybox.Datastore.WinService
{
    public partial class PersistentService : ServiceBase
    {
        #region Class Members

        private static Gravitybox.Datastore.Common.ISystemCore _core = null;
        private bool _echoConsole = false;

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

        public PersistentService(bool echoConsole)
        {
            _echoConsole = echoConsole;
        }

        #endregion

        #region Service Events

        protected override void OnStart(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
            this.Start();
        }

        protected override void OnStop()
        {
            //KillTimer();
            try
            {
                if (_core != null) _core.ShutDown();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
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
                LoggerCQ.LogError(ex);
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

        private static void StartupEndpoint()
        {
            try
            {
                LoggerCQ.LogInfo("Attempting to upgrade database.");
                var connectionStringSettings = ConfigurationManager.ConnectionStrings["DatastoreEntities"];
                var connectionStringBuilder = new SqlConnectionStringBuilder(connectionStringSettings.ConnectionString)
                {
                    InitialCatalog = "Master"
                };

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

                var service = new Gravitybox.Datastore.Server.Core.SystemCore(ConfigurationManager.ConnectionStrings["DatastoreEntities"].ConnectionString);
                var primaryAddress = new Uri("net.tcp://localhost:" + ConfigHelper.Port + "/__datastore_core");
                var primaryHost = new ServiceHost(service, primaryAddress);

                //Initialize the service
                var netTcpBinding = new NetTcpBinding();
                netTcpBinding.Security.Mode = SecurityMode.None;
                primaryHost.AddServiceEndpoint(typeof(Gravitybox.Datastore.Common.ISystemCore), netTcpBinding, string.Empty);
                primaryHost.Open();

                //Create Core Listener
                var primaryEndpoint = new EndpointAddress(primaryHost.BaseAddresses.First().AbsoluteUri);
                var primaryClient = new ChannelFactory<Gravitybox.Datastore.Common.ISystemCore>(netTcpBinding, primaryEndpoint);
                _core = primaryClient.CreateChannel();

                #endregion

                LoadEngine(service);

                LoggerCQ.LogInfo("Service started complete");

                //Initialize instances for fail over
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
            try
            {
                //Load Server Object
                var baseAddress = new Uri("net.tcp://localhost:" + ConfigHelper.Port + "/__datastore_engine");
                var serviceInstance = core.Manager;
                var host = new ServiceHost(serviceInstance, baseAddress);

                //Initialize the service
                var myBinding = new NetTcpBinding()
                {
                    MaxBufferSize = int.MaxValue,
                    MaxReceivedMessageSize = int.MaxValue,
                    MaxBufferPoolSize = 0,
                    ReaderQuotas = new System.Xml.XmlDictionaryReaderQuotas()
                    {
                        MaxArrayLength = int.MaxValue,
                        MaxBytesPerRead = int.MaxValue,
                        MaxDepth = int.MaxValue,
                        MaxNameTableCharCount = int.MaxValue,
                        MaxStringContentLength = int.MaxValue,
                    },
                    OpenTimeout = new TimeSpan(0, 0, 10),
                    ReceiveTimeout = new TimeSpan(0, 0, 120),
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

                host.Open();

            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }

        }

        #endregion

        private void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            try
            {
                LoggerCQ.LogError(e.ExceptionObject as Exception);
            }
            catch (Exception)
            {
                //Do Nothing
            }
        }
    }
}
