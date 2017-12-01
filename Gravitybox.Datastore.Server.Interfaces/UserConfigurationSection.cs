using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace Celeriq.Server.Interfaces
{
    internal class UserConfigurationSection : System.Configuration.ConfigurationSection
    {
        [ConfigurationProperty("Users", IsDefaultCollection = false)]
        [ConfigurationCollection(typeof (UserConfigurationElementCollection),
            AddItemName = "add",
            ClearItemsName = "clear",
            RemoveItemName = "remove")]
        public UserConfigurationElementCollection UserConfiguration
        {
            get { return (UserConfigurationElementCollection) base["Users"]; }
        }
    }
}