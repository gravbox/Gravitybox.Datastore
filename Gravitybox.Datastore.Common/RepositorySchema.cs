#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable()]
    [DataContract]
    [KnownType(typeof(DimensionDefinition))]
    [KnownType(typeof(FieldDefinition))]
    [KnownType(typeof(GeoCode))]
    [KnownType(typeof(GeoCodeFieldFilter))]
    [KnownType(typeof(DerivedField))]
    [KnownType(typeof(DerivedFieldValue))]
    public class RepositorySchema
    {
        /// <summary />
        [Serializable()]
        public enum DataTypeConstants
        {
            /// <summary />
            [EnumMember]
            String,
            /// <summary />
            [EnumMember]
            Int,
            /// <summary />
            [EnumMember]
            DateTime,
            /// <summary />
            [EnumMember]
            Float,
            /// <summary />
            [EnumMember]
            Bool,
            /// <summary />
            [EnumMember]
            GeoCode,
            /// <summary />
            [EnumMember]
            List,
        }

        /// <summary />
        [Serializable()]
        public enum DimensionTypeConstants
        {
            /// <summary />
            [EnumMember]
            Normal,
            /// <summary />
            [EnumMember]
            Range,
            /// <summary />
            [EnumMember]
            List,
        }

        /// <summary />
        [Serializable()]
        public enum FieldTypeConstants
        {
            /// <summary />
            [EnumMember]
            Field,
            /// <summary />
            [EnumMember]
            Dimension,
        }

        /// <summary />
        [Serializable()]
        public enum MultivalueComparisonContants
        {
            /// <summary />
            [EnumMember]
            Union,
            /// <summary />
            [EnumMember]
            Intersect,
        }

        /// <summary />
        [Serializable()]
        public enum FieldIndexingConstants
        {
            /// <summary />
            [EnumMember]
            All,
            /// <summary />
            [EnumMember]
            Minimal,
            /// <summary />
            [EnumMember]
            Progressive,
        }

        /// <summary />
        public RepositorySchema()
        {
            this.FieldList = new List<FieldDefinition>();
            this.ID = Guid.NewGuid();
            this.CreatedDate = DateTime.Now;
            this.FieldIndexing = FieldIndexingConstants.All;
        }

        /// <summary />
        public void Load(string fileName)
        {
            try
            {
                this.ID = Guid.NewGuid();
                var document = new XmlDocument();
                document.Load(fileName);
                this.LoadXml(document.OuterXml);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static RepositorySchema CreateFromXml(string xml)
        {
            try
            {
                var retval = new RepositorySchema();
                retval.LoadXml(xml);
                return retval;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        /// <summary />
        public void LoadXml(string xml)
        {
            try
            {
                var document = new XmlDocument();
                document.LoadXml(xml);
                this.Name = XmlHelper.GetNodeValue(document.DocumentElement, "name", string.Empty);
                this.ObjectAlias = XmlHelper.GetNodeValue(document.DocumentElement, "objectalias", string.Empty);
                this.UserPermissionField = null;

                try
                {
                    this.CreatedDate = DateTime.ParseExact(XmlHelper.GetNodeValue(document.DocumentElement, "createddate", "2010-01-01"), "yyyy-MM-dd HH:mm:ss", null);
                }
                catch (Exception ex)
                {
                    this.CreatedDate = DateTime.ParseExact(XmlHelper.GetNodeValue(document.DocumentElement, "createddate", "2010-01-01"), "yyyy-MM-dd", null);
                }
                this.ID = new Guid(XmlHelper.GetNodeValue(document.DocumentElement, "id", Guid.NewGuid().ToString()));
                var pid = XmlHelper.GetNodeValue(document.DocumentElement, "parentid", string.Empty);
                if (string.IsNullOrEmpty(pid)) this.ParentID = null;
                else this.ParentID = new Guid(pid);
                this.Description = XmlHelper.GetNodeValue(document.DocumentElement, "description", string.Empty);
                this.FieldIndexing = XmlHelper.GetNodeValue<FieldIndexingConstants>(document.DocumentElement, "fieldindexing", this.FieldIndexing);

                this.InternalID = XmlHelper.GetNodeValue(document.DocumentElement, "internalid", this.InternalID);
                this.ChangeStamp = XmlHelper.GetNodeValue(document.DocumentElement, "changestamp", this.ChangeStamp);

                if (document.DocumentElement == null) return;
                var nodeList = document.DocumentElement.SelectNodes("fields/field");
                this.FieldList.Clear();
                foreach (XmlNode node in nodeList)
                {
                    #region Load values from XML

                    var name = XmlHelper.GetAttribute(node, "name", string.Empty);

                    DataTypeConstants dataType;
                    var dtXml = XmlHelper.GetAttribute(node, "datatype", DataTypeConstants.String.ToString());
                    if (!Enum.TryParse<DataTypeConstants>(dtXml, true, out dataType))
                        throw new Exception("Unknown data type: '" + dtXml + "'!");

                    FieldTypeConstants fieldType;
                    var ftXML = XmlHelper.GetAttribute(node, "fieldtype", FieldTypeConstants.Field.ToString());
                    if (!Enum.TryParse<FieldTypeConstants>(ftXML, out fieldType))
                        throw new Exception("Unknown field type: " + ftXML + "!");

                    var isprimarykey = XmlHelper.GetAttribute(node, "isprimarykey", false);
                    var isDataGrouping = XmlHelper.GetAttribute(node, "isdatagrouping", false);
                    var allowNull = XmlHelper.GetAttribute(node, "allownull", true);
                    var ispivot = XmlHelper.GetAttribute(node, "ispivot", false);
                    var pivotGroup = XmlHelper.GetAttribute(node, "pivotgroup", string.Empty);
                    var pivotOrder = XmlHelper.GetAttribute(node, "pivotorder", 0);
                    var description = XmlHelper.GetAttribute(node, "description", string.Empty);
                    var allowIndex = XmlHelper.GetAttribute(node, "allowIndex", true);

                    #endregion

                    FieldDefinition newField = null;
                    if (fieldType == FieldTypeConstants.Dimension)
                    {
                        newField = new DimensionDefinition();
                    }
                    else
                    {
                        newField = new FieldDefinition();
                    }

                    //Setup base field properties
                    if (this.FieldList == null) this.FieldList = new List<FieldDefinition>();
                    this.FieldList.Add(newField);
                    newField.IsPrimaryKey = isprimarykey;
                    newField.IsDataGrouping = isDataGrouping;
                    newField.AllowNull = allowNull;
                    newField.AllowTextSearch = XmlHelper.GetAttribute(node, "allowtextsearch", false);
                    newField.SearchAsc = XmlHelper.GetAttribute(node, "searchasc", true);
                    newField.DataType = dataType;
                    newField.FieldType = fieldType;
                    newField.Length = XmlHelper.GetAttribute(node, "length", 100);
                    newField.Name = name;
                    newField.IsPivot = ispivot;
                    newField.PivotGroup = pivotGroup;
                    newField.PivotOrder = pivotOrder;
                    newField.Description = description;
                    newField.AllowIndex = allowIndex;

                    if (XmlHelper.GetAttribute(node, "userpermission", false))
                    {
                        this.UserPermissionField = newField;
                    }

                    //Setup dimension specific properties
                    if (fieldType == FieldTypeConstants.Dimension)
                    {
                        var dimension = newField as DimensionDefinition;
                        if (dimension != null)
                        {
                            int didx;
                            if (int.TryParse(XmlHelper.GetAttribute(node, "didx", "0"), out didx))
                                dimension.DIdx = didx;

                            dimension.Parent = XmlHelper.GetAttribute(node, "parent", string.Empty);

                            var dtypename = XmlHelper.GetAttribute(node, "dimensiontype", string.Empty);
                            DimensionTypeConstants dtype;
                            if (Enum.TryParse<DimensionTypeConstants>(dtypename, out dtype))
                                dimension.DimensionType = dtype;

                            //Process this only for Int data types
                            if (dimension.DataType == DataTypeConstants.Int && XmlHelper.AttributeExists(node, "numericbreak"))
                            {
                                var nb = XmlHelper.GetAttribute(node, "numericbreak", -1);
                                if (nb > 0) dimension.NumericBreak = nb;
                                else throw new Exception("The numeric break value must be greater than zero!");
                            }
                        }

                    }

                }

            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public void ToDisk(string fileName)
        {
            try
            {
                var document = new XmlDocument();
                document.LoadXml(this.ToXml());
                document.Save(fileName);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public string ToXml(bool useInternals = false)
        {
            try
            {
                var document = new XmlDocument();
                document.LoadXml("<repository></repository>");
                XmlHelper.AddElement(document.DocumentElement, "name", this.Name);
                XmlHelper.AddElement(document.DocumentElement, "objectalias", this.ObjectAlias);
                XmlHelper.AddElement(document.DocumentElement, "createddate", this.CreatedDate.ToString("yyyy-MM-dd HH:mm:ss"));
                XmlHelper.AddElement(document.DocumentElement, "id", this.ID.ToString());
                if (this.ParentID.HasValue)
                    XmlHelper.AddElement(document.DocumentElement, "parentid", this.ParentID.Value.ToString());
                XmlHelper.AddElement(document.DocumentElement, "description", this.Description);
                XmlHelper.AddElement(document.DocumentElement, "fieldindexing", this.FieldIndexing.ToString());

                if (useInternals)
                {
                    XmlHelper.AddElement(document.DocumentElement, "internalid", this.InternalID.ToString());
                    XmlHelper.AddElement(document.DocumentElement, "changestamp", this.ChangeStamp.ToString());
                }

                var fieldListNode = XmlHelper.AddElement(document.DocumentElement, "fields");

                foreach (var field in this.FieldList)
                {
                    var dimensionDef = field as DimensionDefinition;

                    var fieldNode = XmlHelper.AddElement(fieldListNode, "field");
                    XmlHelper.AddAttribute(fieldNode, "name", field.Name);
                    XmlHelper.AddAttribute(fieldNode, "datatype", field.DataType.ToString());
                    XmlHelper.AddAttribute(fieldNode, "fieldtype", field.FieldType.ToString());
                    XmlHelper.AddAttribute(fieldNode, "ispivot", field.IsPivot);
                    XmlHelper.AddAttribute(fieldNode, "pivotgroup", field.PivotGroup);
                    XmlHelper.AddAttribute(fieldNode, "pivotorder", field.PivotOrder);
                    XmlHelper.AddAttribute(fieldNode, "description", field.Description);
                    XmlHelper.AddAttribute(fieldNode, "allowIndex", field.AllowIndex); 

                    //If this is the field to be used for user permissions then mark it as such
                    if (this.UserPermissionField != null && this.UserPermissionField.Name.Match(field.Name))
                    {
                        XmlHelper.AddAttribute(fieldNode, "userpermission", true);
                    }

                    if (field.DataType == DataTypeConstants.String)
                    {
                        XmlHelper.AddAttribute(fieldNode, "length", field.Length);
                        if (field.AllowTextSearch)
                            XmlHelper.AddAttribute(fieldNode, "allowtextsearch", field.AllowTextSearch);
                        if (!field.SearchAsc)
                            XmlHelper.AddAttribute(fieldNode, "searchasc", field.SearchAsc);
                    }

                    if (dimensionDef != null && dimensionDef.DataType == DataTypeConstants.Int && dimensionDef.NumericBreak != null)
                    {
                        XmlHelper.AddAttribute(fieldNode, "numericbreak", dimensionDef.NumericBreak.Value.ToString());
                    }

                    if (dimensionDef != null && !string.IsNullOrEmpty(dimensionDef.Parent))
                    {
                        XmlHelper.AddAttribute(fieldNode, "parent", dimensionDef.Parent);
                    }

                    if (dimensionDef != null)
                    {
                        XmlHelper.AddAttribute(fieldNode, "dimensiontype", dimensionDef.DimensionType.ToString());
                        XmlHelper.AddAttribute(fieldNode, "didx", dimensionDef.DIdx.ToString());
                    }

                    if (field.IsPrimaryKey)
                        XmlHelper.AddAttribute(fieldNode, "isprimarykey", field.IsPrimaryKey);
                    if (field.IsDataGrouping)
                        XmlHelper.AddAttribute(fieldNode, "isdatagrouping", field.IsDataGrouping);
                    XmlHelper.AddAttribute(fieldNode, "allownull", field.AllowNull);
                }
                return XmlHelper.FormatXMLString(document.OuterXml);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public override int GetHashCode()
        {
            var newO = this.Clone();
            newO.FieldList = newO.FieldList.OrderBy(x => x.Name).ToList();
            newO.ObjectAlias = null;
            newO.CreatedDate = new DateTime(2000, 1, 1);
            newO.Name = null;
            newO.Description = null;
            var h = newO.ToXml();
            return (int)EncryptionDomain.HashFast(h);
        }

        /// <summary />
        [DataMember]
        public string Name { get; set; } = string.Empty;

        /// <summary />
        [DataMember]
        public string ObjectAlias { get; set; }

        /// <summary />
        [DataMember]
        public DateTime CreatedDate { get; set; }

        /// <summary />
        [DataMember]
        public Guid ID { get; set; }

        /// <summary />
        [DataMember]
        public Guid? ParentID { get; set; }

        /// <summary />
        [NonSerialized]
        public int InternalID;

        /// <summary />
        [NonSerialized]
        public int ChangeStamp;

        /// <summary />
        [DataMember]
        public string Description { get; set; }

        /// <summary />
        [DataMember]
        public FieldIndexingConstants FieldIndexing { get; set; }

        /// <summary />
        [DataMember]
        public List<FieldDefinition> FieldList { get; set; }

        /// <summary />
        [DataMember]
        public FieldDefinition UserPermissionField { get; set; }

        /// <summary />
        public IEnumerable<DimensionDefinition> DimensionList
        {
            get { return this.FieldList.Where(x => x.FieldType == FieldTypeConstants.Dimension).Cast<DimensionDefinition>().OrderBy(x => x.Name); }
        }

        /// <summary />
        public FieldDefinition PrimaryKey
        {
            get { return this.FieldList.FirstOrDefault(x => x.IsPrimaryKey); }
        }

        /// <summary />
        public override string ToString()
        {
            return $"{this.ID} / {this.Name} / {this.FieldList?.Count}";
        }

        /// <summary />
        public long VersionHash
        {
            get
            {
                try
                {
                    //OLD
                    //var copy = this.Clone();
                    //copy.DimensionList.ToList().ForEach(x => x.DIdx = 0);
                    //var document = new XmlDocument();
                    //document.LoadXml(copy.ToXml());
                    ////XmlHelper.RemoveElement(document, "//name");
                    ////XmlHelper.RemoveElement(document, "//id");
                    ////XmlHelper.RemoveElement(document, "//createddate");
                    ////return EncryptionDomain.Hash(document.OuterXml);
                    //return EncryptionDomain.Hash(document.SelectSingleNode("//fields").OuterXml);

                    //NEW Way - 20x faster
                    var sb = new StringBuilder();
                    if (this.FieldList != null)
                    {
                        foreach (var c in this.FieldList)
                        {
                            sb.Append(c.Hash + "|");
                        }
                    }

                    if (this.ParentID != null)
                        sb.Append(this.ParentID.Value + "|");

                    sb.Append(this.FieldIndexing.ToString() + "|");

                    if (this.UserPermissionField != null)
                        sb.Append(this.UserPermissionField.Name + "|");

                    if (this.DimensionList != null)
                        this.DimensionList.ToList().ForEach(x => sb.Append(x.Name + "|"));

                    if (this.PrimaryKey != null)
                        sb.Append(this.PrimaryKey.Name + "|");

                    if (this.UserPermissionField != null)
                        sb.Append("PERM-" + this.UserPermissionField.Name + "|");

                    sb.Append(this.ID + "|");
                    sb.Append(this.ObjectAlias + "|");
                    sb.Append(this.Name + "|");

                    return EncryptionDomain.HashFast(sb.ToString());
                }
                catch (Exception ex)
                {
                    return 0;
                }
            }
        }

        /// <summary />
        public RepositorySchema Clone()
        {
            try
            {
                var retval = new RepositorySchema();
                retval.LoadXml(this.ToXml());
                retval.InternalID = this.InternalID;
                retval.ChangeStamp = this.ChangeStamp;
                return retval;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

    }
}