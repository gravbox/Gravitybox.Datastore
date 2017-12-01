using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Gravitybox.Datastore.Configuration
{
    public partial class ConnectionControl : UserControl
    {
        public ConnectionControl()
        {
            InitializeComponent();

            optLoginSecurity.CheckedChanged += optLoginSecurity_CheckedChanged;
            optIntegratedSecurity.CheckedChanged += optIntegratedSecurity_CheckedChanged;
            this.UpdateLogin();
        }

        private void optIntegratedSecurity_CheckedChanged(object sender, EventArgs e)
        {
            this.UpdateLogin();
        }

        private void optLoginSecurity_CheckedChanged(object sender, EventArgs e)
        {
            this.UpdateLogin();
        }

        private void UpdateLogin()
        {
            txtPassword.Enabled = !optIntegratedSecurity.Checked;
            txtUserName.Enabled = !optIntegratedSecurity.Checked;
        }

        public ConnectionProperties GetConnectionProperties()
        {
            var retval = new ConnectionProperties();
            retval.Server = txtServer.Text;
            retval.UseIntegratedSecurity = optIntegratedSecurity.Checked;
            retval.UserName = txtUserName.Text;
            retval.Password = txtPassword.Text;
            return retval;
        }

    }
}