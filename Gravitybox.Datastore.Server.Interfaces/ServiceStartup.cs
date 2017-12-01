using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Celeriq.Common;

namespace Celeriq.Server.Interfaces
{
    /// <summary />
    public class ServiceStartup
    {
        private int _cacheLength = 300;

        //public Guid RepositoryKey { get; set; }

        /// <summary>
        /// The length in seconds to hold queries in cache
        /// Use 0 for no caching
        /// </summary>
        public int CacheLength
        {
            get { return _cacheLength; }
            set
            {
                if (value < 0) throw new Exception("The value cannot be less than zero.");
                _cacheLength = value;
            }
        }

    }
}