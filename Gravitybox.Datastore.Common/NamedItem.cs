using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    public class NamedItem
    {
        /// <summary />
        [DataMember]
        public string Key { get; set; }

        /// <summary />
        [DataMember]
        public string Value { get; set; }
    }

    /// <summary />
    [Serializable]
    public class NamedItemList : List<NamedItem>
    {
        /// <summary />
        public void Add(string key, string value)
        {
            this.Add(new NamedItem() { Key = key, Value = value });
        }

        /// <summary />
        public string this[string key]
        {
            get
            {
                var item = this.FirstOrDefault(x => x.Key == key);
                if (item == null) return null;
                else return item.Value;
            }
            set
            {
                var item = this.FirstOrDefault(x => x.Key == key);
                if (item != null) item.Value = value;
                else this.Add(key, value);
            }
        }
    }
}