using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;

namespace HP.Datastore.Common
{
    /// <summary />
    public class CompressedNetTcpBinding : NetTcpBinding
    {
        /// <summary />
        public override System.ServiceModel.Channels.BindingElementCollection CreateBindingElements()
        {
            var elements = base.CreateBindingElements();
            var encodingBinding = elements.Single(e => e is System.ServiceModel.Channels.BinaryMessageEncodingBindingElement);
            ((System.ServiceModel.Channels.BinaryMessageEncodingBindingElement)encodingBinding).CompressionFormat = System.ServiceModel.Channels.CompressionFormat.GZip;

            var bindingElementCollection = new System.ServiceModel.Channels.BindingElementCollection();
            foreach (var bindingElement in elements)
            {
                bindingElementCollection.Add(bindingElement);
            }

            return bindingElementCollection.Clone();
        }
    }

}
