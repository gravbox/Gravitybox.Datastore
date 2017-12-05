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

            var serviceName = "Gravitybox Datastore";
            if (!string.IsNullOrWhiteSpace(serviceName))
            {
                this.serviceInstaller.ServiceName = serviceName;
            }
        }
    }
}