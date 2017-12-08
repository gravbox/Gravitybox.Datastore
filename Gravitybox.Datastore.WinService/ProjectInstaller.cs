using System.ComponentModel;
using System.Configuration;
using System.Configuration.Install;
using System.Reflection;

namespace Gravitybox.Datastore.WinService
{
    [RunInstaller(true)]
    public partial class ProjectInstaller : Installer
    {
        public ProjectInstaller()
        {
            InitializeComponent();

            var serviceName = GetServiceNameAppConfig("serviceName");

            if (!string.IsNullOrWhiteSpace(serviceName))
            {
                this.serviceInstaller.ServiceName = serviceName;
            }
        }

        public string GetServiceNameAppConfig(string serviceName)
        {
            var config = ConfigurationManager.OpenExeConfiguration(Assembly.GetAssembly(typeof(ProjectInstaller)).Location);
            return config.AppSettings.Settings[serviceName].Value;
        }
    }
}