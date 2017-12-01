using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    [DataContract()]
    public class UserCredentials : ICloneable
    {
        /// <summary />
        [DataMember]
        public string UserName { get; set; }
        /// <summary />
        [DataMember]
        public string Password { get; set; }

        /// <summary />
        public override string ToString()
        {
            return this.UserName;
        }

        #region ICloneable Members

        object ICloneable.Clone()
        {
            var retval = new UserCredentials();
            retval.Password = this.Password;
            retval.UserName = this.UserName;
            return retval;
        }

        #endregion
    }
}