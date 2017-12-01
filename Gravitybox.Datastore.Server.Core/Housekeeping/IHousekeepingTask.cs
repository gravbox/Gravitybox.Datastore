using Gravitybox.Datastore.EFDAL.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.Housekeeping
{
    public interface IHousekeepingTask
    {
        HousekeepingTaskType Type { get; }

        bool Run();
    }
}
