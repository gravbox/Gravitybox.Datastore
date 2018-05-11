using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Gravitybox.Datastore.Server.Core
{
    [Serializable]
    [XmlRootAttribute(ElementName = "configuration")]
    public class SetupConfig
    {
        public const string YFileGroup = "YGroup";
        public const string IndexFileGroup = "IDXGroup";

        [XmlElement(ElementName = "listdatapath")]
        public string ListDataPath { get; set; }

        [XmlElement(ElementName = "indexpath")]
        public string IndexPath { get; set; }

        public bool HashListTableFileGroup { get { return !string.IsNullOrEmpty(this.ListDataPath); } }

        public bool HashIndexFileGroup { get { return !string.IsNullOrEmpty(this.IndexPath); } }
    }
}