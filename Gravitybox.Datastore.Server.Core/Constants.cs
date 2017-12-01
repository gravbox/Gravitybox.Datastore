using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core
{
    internal static class Constants
    {
        //Normal repositories use this numbering scheme
        public const long DGROUP = 1000000; // Dimension groups start at 1 Million
        public const long DVALUEGROUP = DGROUP * 10; // Refinement values are grouped by 10 Million

        //Child/Extension repositories use this numbering scheme
        public const long DGROUPEXT = 1; // Dimension groups start at 1
        public const long DVALUEGROUPEXT = 10000; // Refinement values are grouped by 10K
    }
}
