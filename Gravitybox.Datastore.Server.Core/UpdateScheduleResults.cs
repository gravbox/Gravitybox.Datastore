using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core
{
    internal class UpdateScheduleResults
    {
        public bool HasChanged { get; set; } = false;
        public bool FtsChanged { get; set; } = false;
        public List<string> Errors { get; set; } = new List<string>();
    }

}
