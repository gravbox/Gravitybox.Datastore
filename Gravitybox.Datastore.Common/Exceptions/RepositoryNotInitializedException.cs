using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Common.Exceptions
{
    /// <summary />
    [Serializable]
    public class RepositoryNotInitializedException : System.Exception
    {
        /// <summary />
        public RepositoryNotInitializedException(Guid repositoryId) : base()
        {
            this.RepositoryId = repositoryId;
        }

        /// <summary />
        public Guid RepositoryId { get; set; }

        /// <summary />
        public override string ToString()
        {
            return "The repository '" + this.RepositoryId + "' was not initialized.";
        }
    }

}
