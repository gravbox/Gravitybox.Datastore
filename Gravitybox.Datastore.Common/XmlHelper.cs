#pragma warning disable 0168
using System;
using System.Linq;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.XPath;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public static class XmlHelper
    {
        /// <summary />
        public static XPathNavigator CreateXPathNavigator(XmlReader reader)
        {
            var document = new XPathDocument(reader);
            return document.CreateNavigator();
        }

        /// <summary />
        public static XPathNodeIterator GetIterator(XPathNavigator navigator, string xPath)
        {
            return (XPathNodeIterator) navigator.Evaluate(xPath);
        }

        #region GetXmlReader

        /// <summary />
        public static XmlReader GetXmlReader(FileInfo fileInfo)
        {
            var textReader = new XmlTextReader(fileInfo.FullName);
            return textReader;
        }

        #endregion

        #region GetNode

        /// <summary />
        public static System.Xml.XmlNode GetNode(this System.Xml.XmlNode node, string XPath)
        {
            try
            {
                if ((XPath + string.Empty).Contains(":"))
                {
                    foreach (XmlNode n in node.ChildNodes)
                    {
                        if (n.Name == XPath)
                            return n;
                    }
                }
                else
                {
                    return node.SelectSingleNode(XPath);
                }
                return null;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static System.Xml.XmlNode GetNode(this System.Xml.XmlNode xmlNode, string XPath, XmlNamespaceManager nsManager)
        {
            try
            {
                System.Xml.XmlNode node = null;
                node = xmlNode.SelectSingleNode(XPath, nsManager);
                return node;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        #endregion

        #region GetNodeValue

        /// <summary />
        public static string GetNodeValue(this System.Xml.XmlDocument document, string XPath, string defaultValue)
        {
            try
            {
                return GetNodeValue(document.DocumentElement, XPath, defaultValue);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static string GetNodeValue(this System.Xml.XmlNode element, string XPath, string defaultValue)
        {
            return GetNodeValue(element, XPath, null, defaultValue);
        }

        /// <summary />
        public static string GetNodeValue(this System.Xml.XmlNode element, string XPath, XmlNamespaceManager nsManager, string defaultValue)
        {
            try
            {
                XmlNode node = null;
                if (nsManager == null)
                    node = element.SelectSingleNode(XPath);
                else
                    node = element.SelectSingleNode(XPath, nsManager);

                if (node == null)
                    return defaultValue;
                else
                    return node.InnerText;
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        /// <summary />
        public static string GetNodeValue(this System.Xml.XmlNode element, string[] XPathList, string defaultValue)
        {
            try
            {
                if (XPathList == null || XPathList.Length == 0)
                    return defaultValue;

                foreach (var str in XPathList)
                {
                    var node = GetNode(element, str);
                    if (node != null) return node.InnerText;
                }
                return defaultValue;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static int GetNodeValue(this System.Xml.XmlNode element, string XPath, int defaultValue)
        {
            try
            {
                var node = GetNode(element, XPath);
                if (node == null)
                    return defaultValue;
                else
                    return int.Parse(node.InnerText);
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        /// <summary />
        public static int? GetNodeValue(this System.Xml.XmlNode element, string XPath, int? defaultValue)
        {
            try
            {
                var node = GetNode(element, XPath);
                if (node == null)
                    return defaultValue;
                else
                    return int.Parse(node.InnerText);
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        /// <summary />
        public static Single GetNodeValue(this System.Xml.XmlNode element, string XPath, Single defaultValue)
        {
            try
            {
                var node = GetNode(element, XPath);
                if (node == null)
                    return defaultValue;
                else
                    return Single.Parse(node.InnerText);
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        /// <summary />
        public static Single? GetNodeValue(this System.Xml.XmlNode element, string XPath, Single? defaultValue)
        {
            try
            {
                var node = GetNode(element, XPath);
                if (node == null)
                    return defaultValue;
                else
                    return Single.Parse(node.InnerText);
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        /// <summary />
        public static double GetNodeValue(this System.Xml.XmlNode element, string XPath, double defaultValue)
        {
            try
            {
                var node = GetNode(element, XPath);
                if (node == null)
                    return defaultValue;
                else
                    return double.Parse(node.InnerText);
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        /// <summary />
        public static double? GetNodeValue(this System.Xml.XmlNode element, string XPath, double? defaultValue)
        {
            try
            {
                var node = GetNode(element, XPath);
                if (node == null)
                    return defaultValue;
                else
                    return double.Parse(node.InnerText);
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        /// <summary />
        public static bool GetNodeValue(this System.Xml.XmlNode element, string XPath, bool defaultValue)
        {
            try
            {
                var node = GetNode(element, XPath);
                if (node == null)
                    return defaultValue;
                else
                    return bool.Parse(node.InnerText);
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        /// <summary />
        public static bool? GetNodeValue(this System.Xml.XmlNode element, string XPath, bool? defaultValue)
        {
            try
            {
                var node = GetNode(element, XPath);
                if (node == null)
                    return defaultValue;
                else
                    return bool.Parse(node.InnerText);
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        /// <summary />
        public static DateTime GetNodeValue(this System.Xml.XmlNode element, string XPath, DateTime defaultValue)
        {
            try
            {
                var node = GetNode(element, XPath);
                if (node == null)
                    return defaultValue;
                else
                    return DateTime.Parse(node.InnerText);
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        /// <summary />
        public static DateTime? GetNodeValue(this System.Xml.XmlNode element, string XPath, DateTime? defaultValue)
        {
            try
            {
                var node = GetNode(element, XPath);
                if (node == null)
                    return defaultValue;
                else
                    return DateTime.Parse(node.InnerText);
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        /// <summary />
        public static string GetNodeValue(this System.Xml.XmlDocument document, string XPath, XmlNamespaceManager nsManager, string defaultValue)
        {
            try
            {
                return GetNodeValue(document.DocumentElement, XPath, nsManager, defaultValue);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static TEnum GetNodeValue<TEnum>(this System.Xml.XmlNode element, string XPath, TEnum defaultValue)
            where TEnum : struct
        {
            try
            {
                var node = GetNode(element, XPath);
                if (node == null)
                    return defaultValue;
                else
                {
                    TEnum t;
                    if (Enum.TryParse(node.InnerText, out t))
                        return t;
                    else
                        return defaultValue;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        #endregion

        #region GetNodeXML

        /// <summary />
        public static string GetNodeXML(this XmlDocument document, string XPath, string defaultValue, bool useOuter)
        {
            try
            {
                XmlNode node = null;
                node = document.SelectSingleNode(XPath);
                if (node == null)
                    return defaultValue;
                else if (useOuter)
                    return node.OuterXml;
                else
                    return node.InnerXml;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static string GetNodeXML(this XmlDocument document, string XPath, string defaultValue)
        {
            return GetNodeXML(document, XPath, defaultValue, false);
        }

        #endregion

        #region GetAttributeValue

        /// <summary />
        public static bool AttributeExists(this XmlNode node, string attributeName)
        {
            return (node.Attributes[attributeName] != null);
        }

        /// <summary />
        public static string GetAttribute(this XmlNode node, string attributeName)
        {
            return GetAttribute(node, attributeName, "");
        }

        /// <summary />
        public static string GetAttribute(this XmlNode node, string attributeName, string defaultValue)
        {
            var attr = node.Attributes[attributeName];
            if (attr == null)
                attr = node.Attributes[attributeName.ToLower()];

            if (attr == null)
                return defaultValue;
            else
                return attr.Value;
        }

        /// <summary />
        public static Guid GetAttribute(this XmlNode node, string attributeName, Guid defaultValue)
        {
            var attr = node.Attributes[attributeName];
            if (attr == null)
                attr = node.Attributes[attributeName.ToLower()];

            if (attr == null)
                return defaultValue;
            else
                return new Guid(attr.Value);
        }

        /// <summary />
        public static double GetAttribute(this XmlNode node, string attributeName, double defaultValue)
        {
            var attr = node.Attributes[attributeName];
            if (attr == null)
                attr = node.Attributes[attributeName.ToLower()];

            if (attr == null)
                return defaultValue;
            else
                return double.Parse(attr.Value);
        }

        /// <summary />
        public static int GetAttribute(this XmlNode node, string attributeName, int defaultValue)
        {
            var attr = node.Attributes[attributeName];
            if (attr == null)
                attr = node.Attributes[attributeName.ToLower()];

            if (attr == null)
                return defaultValue;
            else
                return int.Parse(attr.Value);
        }

        /// <summary />
        public static long GetAttribute(this XmlNode node, string attributeName, long defaultValue)
        {
            var attr = node.Attributes[attributeName];
            if (attr == null)
                attr = node.Attributes[attributeName.ToLower()];

            if (attr == null)
                return defaultValue;
            else
                return long.Parse(attr.Value);
        }

        /// <summary />
        public static bool GetAttribute(this XmlNode node, string attributeName, bool defaultValue)
        {
            var attr = node.Attributes[attributeName];
            if (attr == null)
                attr = node.Attributes[attributeName.ToLower()];

            if (attr == null)
                return defaultValue;
            else
                return bool.Parse(attr.Value);
        }

        #endregion

        #region AddElement

        /// <summary />
        public static XmlNode AddElement(this XmlElement element, string name, string value)
        {
            XmlDocument document = null;
            XmlElement elemNew = null;

            document = element.OwnerDocument;
            elemNew = document.CreateElement(name);
            if (!string.IsNullOrEmpty(value))
                elemNew.InnerText = value;
            return element.AppendChild(elemNew);
        }

        /// <summary />
        public static XmlNode AddElement(this XmlDocument document, string name, string value)
        {
            var elemNew = document.CreateElement(name);
            if (!string.IsNullOrEmpty(value))
                elemNew.InnerText = value;
            return document.AppendChild(elemNew);
        }

        /// <summary />
        public static XmlNode AddElement(this XmlNode element, string name, string value)
        {
            return AddElement((XmlElement) element, name, value);
        }

        /// <summary />
        public static XmlNode AddElement(this XmlElement element, string name)
        {
            return AddElement((XmlNode) element, name);
        }

        /// <summary />
        public static XmlNode AddElement(this XmlNode element, string name)
        {
            XmlDocument document = null;
            XmlElement elemNew = null;
            document = element.OwnerDocument;
            elemNew = document.CreateElement(name);
            return element.AppendChild(elemNew);
        }

        /// <summary />
        public static XmlNode AddElement(this XmlDocument xmlDocument, string name)
        {
            XmlElement elemNew = null;
            elemNew = xmlDocument.CreateElement(name);
            return xmlDocument.AppendChild(elemNew);
        }

        #endregion

        #region AddAttribute

        /// <summary />
        public static XmlAttribute AddAttribute(this XmlNode node, string name, string value)
        {
            XmlDocument docOwner = null;
            XmlAttribute attrNew = null;

            docOwner = node.OwnerDocument;
            attrNew = docOwner.CreateAttribute(name);
            attrNew.InnerText = value;
            node.Attributes.Append(attrNew);
            return attrNew;
        }

        /// <summary />
        public static XmlAttribute AddAttribute(this XmlElement node, string name, bool value)
        {
            return AddAttribute(node, name, value.ToString());
        }

        /// <summary />
        public static XmlAttribute AddAttribute(this XmlElement node, string name, double value)
        {
            return AddAttribute(node, name, value.ToString());
        }

        /// <summary />
        public static XmlAttribute AddAttribute(this XmlElement node, string name, Guid value)
        {
            return AddAttribute(node, name, value.ToString());
        }

        /// <summary />
        public static XmlAttribute AddAttribute(this XmlElement node, string name, int value)
        {
            return AddAttribute(node, name, value.ToString());
        }

        /// <summary />
        public static XmlAttribute AddAttribute(this XmlNode node, string name, bool value)
        {
            return AddAttribute(node, name, value.ToString());
        }

        /// <summary />
        public static XmlAttribute AddAttribute(this XmlNode node, string name, double value)
        {
            return AddAttribute(node, name, value.ToString());
        }

        /// <summary />
        public static XmlAttribute AddAttribute(this XmlNode node, string name, Guid value)
        {
            return AddAttribute(node, name, value.ToString());
        }

        /// <summary />
        public static XmlAttribute AddAttribute(this XmlNode node, string name, int value)
        {
            return AddAttribute(node, name, value.ToString());
        }

        /// <summary />
        public static XmlAttribute AddAttribute(this XmlElement node, string name, string value)
        {
            return AddAttribute((XmlNode) node, name, value);
        }

        /// <summary />
        public static XmlAttribute AddAttribute(this XmlNode node, string name, long value)
        {
            return AddAttribute(node, name, value.ToString());
        }

        #endregion

        #region RemoveElement

        /// <summary />
        public static void RemoveElement(this XmlDocument document, string XPath)
        {
            XmlNode parentNode = null;
            XmlNodeList nodes = null;

            nodes = document.SelectNodes(XPath);
            if (nodes != null)
            {
                foreach (XmlElement node in nodes)
                {
                    if (node != null)
                    {
                        parentNode = node.ParentNode;
                        node.RemoveAll();
                        parentNode.RemoveChild(node);
                    }
                }
            }
        }

        /// <summary />
        public static void RemoveElement(this XmlElement element)
        {
            var parentNode = element.ParentNode;
            parentNode.RemoveChild(element);
        }

        /// <summary />
        public static void RemoveAttribute(this XmlElement element, string attributeName)
        {
            XmlAttribute attrDelete = null;
            attrDelete = (XmlAttribute) element.Attributes.GetNamedItem(attributeName);
            if (attrDelete == null) return;
            element.Attributes.Remove(attrDelete);
        }

        #endregion

        #region UpdateElement

        /// <summary />
        public static void UpdateElement(this XmlElement element, string newValue)
        {
            element.InnerText = newValue;
        }

        /// <summary />
        public static void UpdateElement(ref XmlDocument XMLDocument, string Xpath, string newValue)
        {
            XMLDocument.SelectSingleNode(Xpath).InnerText = newValue;
        }

        /// <summary />
        public static void UpdateAttribute(this XmlElement XmlElement, string attributeName, string newValue)
        {
            XmlAttribute attrTemp = null;
            attrTemp = (XmlAttribute) XmlElement.Attributes.GetNamedItem(attributeName);
            attrTemp.InnerText = newValue;
        }

        #endregion

        #region GetElement

        /// <summary>
        /// Return first found match in list
        /// </summary>
        public static XmlElement GetElement(this XmlElement parentElement, string[] tagNameList)
        {
            if (tagNameList == null || tagNameList.Length == 0)
                return null;
            foreach(var str in tagNameList)
            {
                var retval = GetElement(parentElement, str);
                if (retval != null) return retval;
            }
            return null;
        }

        /// <summary />
        public static XmlElement GetElement(this XmlElement parentElement, string tagName)
        {
            var list = parentElement.GetElementsByTagName(tagName);
            if (list.Count > 0)
                return (XmlElement) list[0];
            else
                return null;
        }

        #endregion

        #region GetChildValue

        /// <summary />
        public static string GetChildValue(this XmlNode parentNode, string childName)
        {
            return GetChildValue(parentNode, childName, null);
        }

        /// <summary />
        public static double GetChildValue(this XmlNode parentNode, string childName, double defaultValue)
        {
            var node = parentNode.SelectSingleNode(childName);
            if (node != null)
                return double.Parse(node.InnerText);
            else
                return defaultValue;
        }

        /// <summary />
        public static int GetChildValue(this XmlNode parentNode, string childName, int defaultValue)
        {
            var node = parentNode.SelectSingleNode(childName);
            if (node != null)
                return int.Parse(node.InnerText);
            else
                return defaultValue;
        }

        /// <summary />
        public static bool GetChildValue(this XmlNode parentNode, string childName, bool defaultValue)
        {
            var node = parentNode.SelectSingleNode(childName);
            if (node != null)
                return bool.Parse(node.InnerText);
            else
                return defaultValue;
        }

        /// <summary />
        public static Guid GetChildValue(this XmlNode parentNode, string childName, Guid defaultValue)
        {
            var node = parentNode.SelectSingleNode(childName);
            if (node != null)
                return new Guid(node.InnerText);
            else
                return defaultValue;
        }

        /// <summary />
        public static string GetChildValue(this XmlNode parentNode, string childName, string defaultValue)
        {
            var node = parentNode.SelectSingleNode(childName);
            if (node != null)
                return node.InnerText;
            else
                return defaultValue;
        }

        #endregion

        #region FormatXMLString

        /// <summary />
        public static string FormatXMLString(string xml)
        {
            var xd = new XmlDocument();
            xd.LoadXml(xml);
            var sb = new StringBuilder();
            var sw = new StringWriter(sb);
            XmlTextWriter xtw = null;
            try
            {
                xtw = new XmlTextWriter(sw);
                xtw.Formatting = Formatting.Indented;
                xd.WriteTo(xtw);
            }
            finally
            {
                if (xtw != null)
                    xtw.Close();
            }
            return sb.ToString();
        }

        #endregion

    }
}