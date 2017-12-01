using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace Celeriq.Server.Interfaces
{
    internal class UserConfigurationElement : System.Configuration.ConfigurationElement
    {
        public UserConfigurationElement()
            : base()
        {
        }

        public UserConfigurationElement(string userName, string userId, string password)
            : this()
        {
            this.UserName = UserName;
            this.UserId = UserId;
            this.Password = password;
        }

        //[ConfigurationProperty("UserName", DefaultValue = "", IsRequired = true, IsKey = true)]
        //public string UserName { get; set; }

        //[ConfigurationProperty("Password", DefaultValue = "", IsRequired = true, IsKey = false)]
        //public string Password { get; set; }

        //[ConfigurationProperty("UserId", DefaultValue = "", IsRequired = true, IsKey = false)]
        //public string UserId { get; set; }

        [ConfigurationProperty("UserName", DefaultValue = "", IsRequired = true, IsKey = true)]
        public string UserName
        {
            get { return (string) base["UserName"]; }
            set { base["UserName"] = value; }
        }

        [ConfigurationProperty("Password", DefaultValue = "", IsRequired = true, IsKey = false)]
        public string Password {
            get { return (string)base["Password"]; }
            set { base["Password"] = value; }
        }

        [ConfigurationProperty("UserId", DefaultValue = "", IsRequired = true, IsKey = false)]
        public string UserId
        {
            get { return (string)base["UserId"]; }
            set { base["UserId"] = value; }
        }

    }
}
