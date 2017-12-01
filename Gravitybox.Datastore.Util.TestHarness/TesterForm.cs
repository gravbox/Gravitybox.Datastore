using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.Common.Queryable;

namespace Gravitybox.Datastore.Util.TestHarness
{
    public partial class TesterForm : Form
    {
        public TesterForm()
        {
            InitializeComponent();
        }

        private void cmdRun_Click(object sender, EventArgs e)
        {
            txtResults.Text = "(Processing...)";
            try
            {
                if (Guid.TryParse(txtID.Text, out Guid g))
                {
                    using (var repository = new DatastoreRepository<MyItem>(g))
                    {
                        var result = repository.Query
                            .WhereUrl(txtQuery.Text)
                            .Results();

                        var sb = new StringBuilder();
                        sb.AppendLine($"ComputeTime={DateTime.Now.ToString("HH:mm:ss")}");
                        sb.AppendLine($"ComputeTime={result.Diagnostics.ComputeTime}");
                        sb.AppendLine($"Records={result.TotalRecordCount}");
                        sb.AppendLine($"DimensionCount={result.AllDimensions.Count}");
                        sb.AppendLine($"CacheHit={result.Diagnostics.CacheHit}");
                        txtResults.Text = sb.ToString();
                    }
                }
                else
                {
                    txtResults.Text = "(Error)";
                }
            }
            catch (Exception ex)
            {
                txtResults.Text = ex.ToString();
            }
        }

    }

}
