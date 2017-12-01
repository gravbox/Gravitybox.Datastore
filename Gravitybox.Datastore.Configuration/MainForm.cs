using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace HP.Celeriq.Configuration
{
    public partial class MainForm : Form
    {
        private enum WizardStageConstants
        {
            Connect = 0,
            SetupNew = 1,
            FileGroups = 2,
            Summary = 3,
        }

        public MainForm()
        {
            InitializeComponent();

            wizard1.BeforeSwitchPages += wizard1_BeforeSwitchPages;
            wizard1.AfterSwitchPages += wizard1_AfterSwitchPages;
        }

        private void wizard1_AfterSwitchPages(object sender, nHydrate.Wizard.Wizard.AfterSwitchPagesEventArgs e)
        {
            
        }

        private void wizard1_BeforeSwitchPages(object sender, nHydrate.Wizard.Wizard.BeforeSwitchPagesEventArgs e)
        {
            
        }
    }
}
