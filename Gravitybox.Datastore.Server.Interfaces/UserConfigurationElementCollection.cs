using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace Celeriq.Server.Interfaces
{
    internal class UserConfigurationElementCollection : ConfigurationElementCollection
    {
        public UserConfigurationElementCollection()
        {
        }

        public UserConfigurationElement this[int index]
        {
            get { return (UserConfigurationElement) BaseGet(index); }
            set
            {
                if (BaseGet(index) != null)
                {
                    BaseRemoveAt(index);
                }
                BaseAdd(index, value);
            }
        }

        public void Add(UserConfigurationElement item)
        {
            BaseAdd(item);
        }

        public void Clear()
        {
            BaseClear();
        }

        protected override ConfigurationElement CreateNewElement()
        {
            return new UserConfigurationElement();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((UserConfigurationElement) element).UserName;
        }

        public void Remove(UserConfigurationElement item)
        {
            BaseRemove(item.UserName);
        }

        public void RemoveAt(int index)
        {
            BaseRemoveAt(index);
        }

        public void Remove(string name)
        {
            BaseRemove(name);
        }
    }
}
