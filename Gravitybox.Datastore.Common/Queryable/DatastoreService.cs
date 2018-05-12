using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.ServiceModel;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    internal class DatastoreService : IDisposable
    {
        /// <summary />
        public Guid RepositoryId { get; set; }

        /// <summary />
        public string ServerName { get; set; }

        /// <summary />
        public int Port { get; set; }

        /// <summary />
        protected IDataModel DataModelService { get; private set; }

        private ChannelFactory<IDataModel> ChannelFactory { get; set; }

        /// <summary />
        public DatastoreService(Guid repositoryId, string serverName = "localhost", int port = 1973)
        {
            this.RepositoryId = repositoryId;
            this.ServerName = serverName;
            this.Port = port;
            CreateConnection();
        }

        /// <summary />
        ~DatastoreService()
        {
            Dispose(false);
        }

        /// <summary />
        protected virtual void CreateConnection()
        {
            this.ChannelFactory = SystemCoreInteractDomain.GetRepositoryFactory(this.ServerName, this.Port);
            this.DataModelService = this.ChannelFactory.CreateChannel();
            (this.DataModelService as IContextChannel).OperationTimeout = new TimeSpan(0, 0, 120); //Timeout=2m
        }

        /// <summary />
        public bool RepositoryExists()
        {
            ValidateService();

            using (var serverFactory = SystemCoreInteractDomain.GetCoreFactory(ServerName, Port))
            {
                var core = serverFactory.CreateChannel();
                return core.RepositoryExists(this.RepositoryId);
            }
        }

        private RepositorySchema _repositorySchema;

        /// <summary />
        public RepositorySchema Schema
        {
            get
            {
                if (_repositorySchema == null)
                {
                    _repositorySchema = GetSchema();
                }
                return _repositorySchema;
            }
            internal protected set
            {
                _repositorySchema = value;

            }
        }

        /// <summary />
        public virtual bool DeleteRepository()
        {
            using (var serverFactory = SystemCoreInteractDomain.GetCoreFactory(ServerName, Port))
            {
                var core = serverFactory.CreateChannel();
                (core as IContextChannel).OperationTimeout = new TimeSpan(0, 0, 120); //Timeout=2m
                if (core.RepositoryExists(this.RepositoryId))
                {
                    core.DeleteRepository(new RepositorySchema { ID = this.RepositoryId });
                }
            }
            return true;
        }

        /// <summary />
        public virtual ActionDiagnostics ClearRepository()
        {
            ValidateService();
            return DataModelService.Clear(this.RepositoryId);
        }

        /// <summary />
        public virtual int GetTimestamp()
        {
            return DataModelService.GetTimestamp();
        }

        private RepositorySchema GetSchema()
        {
            ValidateService();
            return DataModelService.GetSchema(this.RepositoryId);
        }

        /// <summary />
        public ActionDiagnostics UpdateSchema(RepositorySchema schema)
        {
            if (schema == null) throw new Exception("The schema is not set.");
            if (schema.ID != this.RepositoryId) throw new Exception("The schema does not match the RepositoryID.");

            var result = DataModelService.UpdateSchema(schema);
            if (result?.Errors?.Length > 0)
            {
                throw new Exception(result.Errors.First());
            }
            return result;
        }

        /// <summary />
        public virtual SummarySliceValue CalculateSlice(SummarySlice query)
        {
            ValidateService();
            return DataModelService.CalculateSlice(this.RepositoryId, query);
        }

        /// <summary />
        public virtual DataQueryResults Query(DataQuery query)
        {
            ValidateService();

            if (query.RecordsPerPage <= 200)
            {
                return DataModelService.Query(this.RepositoryId, query);
            }
            else
            {
                //If a large # of records is expected then use compress and stream API
                var v = DataModelService.QueryAndStream(this.RepositoryId, query);
                return v.BinToObject<DataQueryResults>();
            }
        }

        /// <summary />
        public virtual bool IsServerAlive()
        {
            return DataModelService.IsServerAlive();
        }

        /// <summary />
        public virtual bool IsServerMaster()
        {
            return DataModelService.IsServerMaster();
        }

        /// <summary />
        public virtual bool ResetMaster()
        {
            return DataModelService.ResetMaster();
        }

        internal virtual Guid QueryAsync(DataQuery query)
        {
            ValidateService();
            return DataModelService.QueryAsync(this.RepositoryId, query);
        }

        internal virtual bool QueryAsyncReady(Guid hookId)
        {
            ValidateService();

            //Try multiple times in case of communication error
            var retval = false;
            RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                .Execute(() =>
                {
                    retval = DataModelService.QueryAsyncReady(hookId);
                });
            return retval;
        }

        internal virtual byte[] QueryAsyncDownload(Guid hookId, long chunk)
        {
            ValidateService();
            return DataModelService.QueryAsyncDownload(hookId, chunk);
        }

        /// <summary />
        public virtual long GetDataVersion()
        {
            ValidateService();
            return DataModelService.GetDataVersion(this.RepositoryId);
        }

        /// <summary />
        public virtual bool ResetDimensionValue(long dvidx, string value)
        {
            ValidateService();
            return DataModelService.ResetDimensionValue(this.RepositoryId, dvidx, value);
        }

        /// <summary />
        public virtual bool DeleteDimensionValue(long dvidx)
        {
            ValidateService();
            return DataModelService.DeleteDimensionValue(this.RepositoryId, dvidx);
        }

        /// <summary />
        public virtual void CreateRepository(RepositorySchema repositorySchema)
        {
            ValidateService();

            if (repositorySchema.ID == Guid.Empty)
                throw new Exception("Invalid ID for repository");
            if (RepositoryExists())
                return;

            using (var factory = SystemCoreInteractDomain.GetCoreFactory(ServerName, Port))
            {
                var core = factory.CreateChannel();
                (core as IContextChannel).OperationTimeout = new TimeSpan(0, 0, 120); //Timeout=2m
                core.SaveRepository(repositorySchema);
            }
        }

        /// <summary />
        public static RepositorySchema LoadSchemaTemplate(Type datastoreType)
        {
            var schema = DatastoreService.LoadSchemaForType(datastoreType);
            schema.ID = schema.ID; //RepositoryId;
            if (string.IsNullOrEmpty(schema.Name))
                schema.Name = string.Format("{0}-{1}", schema.Name[0], schema.ID); //RepositoryId
            schema.CreatedDate = DateTime.Now;
            return schema;
        }

        /// <summary />
        internal static List<FieldDefinition> GetFields(Type datastoreType)
        {
            var schema = DatastoreService.LoadSchemaForType(datastoreType, true);
            if (schema != null)
                return schema.FieldList;
            return null;
        }

        private void ValidateService()
        {
            if (DataModelService == null)
                throw new InvalidOperationException("The IDataModel service is not set.");
        }

        /// <summary />
        public virtual ActionDiagnostics UpdateData(IEnumerable<DataItem> dataItems)
        {
            ValidateService();
            var schema = Schema;
            if (schema == null) throw new Exception("The schema is not set.");
            if (schema.ID != this.RepositoryId) throw new Exception("The schema does not match the RepositoryID.");
            return DataModelService.UpdateData(schema, dataItems);
        }

        /// <summary />
        public virtual ActionDiagnostics DeleteData(DataQuery query)
        {
            ValidateService();
            var schema = Schema;
            if (schema == null) throw new Exception("The schema is not set.");
            if (schema.ID != this.RepositoryId) throw new Exception("The schema does not match the RepositoryID.");
            return DataModelService.DeleteData(schema, query);
        }

        /// <summary />
        public virtual ActionDiagnostics UpdateDataWhere(DataQuery query, List<DataFieldUpdate> list)
        {
            ValidateService();
            var schema = Schema;
            if (schema == null) throw new Exception("The schema is not set.");
            if (schema.ID != this.RepositoryId) throw new Exception("The schema does not match the RepositoryID.");
            return DataModelService.UpdateDataWhere(schema, query, list);
        }

        /// <summary />
        public virtual void AddPermissions(IEnumerable<PermissionItem> list)
        {
            DataModelService.AddPermission(this.RepositoryId, list);
        }

        /// <summary />
        public virtual void DeletePermissions(IEnumerable<PermissionItem> list)
        {
            DataModelService.DeletePermission(this.RepositoryId, list);
        }

        /// <summary />
        public virtual void ClearPermissions(string fieldValue)
        {
            DataModelService.ClearPermissions(this.RepositoryId, fieldValue);
        }

        public virtual void ClearUserPermissions(int userId)
        {
            DataModelService.ClearUserPermissions(this.RepositoryId, userId);
        }

        internal static RepositorySchema LoadSchemaForType(Type dsType, bool inheritedFieldList = false)
        {
            if (dsType == null)
                throw new Exception("The type must be set");

            if (!dsType.GetInterfaces().Any(x => x.Name == typeof(IDatastoreItem).Name))
                throw new Exception($"The item must implement {typeof(IDatastoreItem).Name}.");

            var dsRepositoryAttribute = dsType.GetCustomAttributes().FirstOrDefault(x => x is DatastoreRepositoryAttribute) as DatastoreRepositoryAttribute;
            if (dsRepositoryAttribute == null)
                throw new InvalidOperationException("Cannot create repository for the specified type. Missing DataStoreRepositoryAttribute.");

            var schema = new RepositorySchema();
            schema.Name = dsRepositoryAttribute.Name;
            schema.ObjectAlias = dsRepositoryAttribute.ObjectAlias;

            Guid rid;
            Guid.TryParse(dsRepositoryAttribute.Id, out rid);
            if (rid == Guid.Empty) rid = Guid.NewGuid();
            schema.ID = rid;

            if (!string.IsNullOrEmpty(dsRepositoryAttribute.ParentId))
                schema.ParentID = new Guid(dsRepositoryAttribute.ParentId);
            schema.FieldIndexing = dsRepositoryAttribute.FieldIndexing;

            //var properties = GetTemplateFields(dsType);
            var properties = new List<MemberInfo>();
            properties.AddRange(dsType.GetProperties());
            properties.AddRange(dsType.GetFields());
            #region Loop Fields
            foreach (MemberInfo prop in properties)
            {
                var field = prop.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(DatastoreFieldAttribute)) as CustomAttributeData;
                if (field != null)
                {
                    var isNullable = false;
                    var fieldDefition = default(FieldDefinition);
                    var defFieldType = RepositorySchema.DataTypeConstants.String;
                    var defLength = 100;
                    var propertyTypeName = "";
                    if (prop is PropertyInfo)
                    {
                        propertyTypeName = ((PropertyInfo)prop).PropertyType.Name;
                        if (propertyTypeName.Contains("Nullable"))
                        {
                            isNullable = true;
                            propertyTypeName = ((PropertyInfo)prop).PropertyType.GenericTypeArguments.Select(x => x.Name).FirstOrDefault();
                        }
                    }
                    else if (prop is FieldInfo)
                    {
                        propertyTypeName = ((FieldInfo)prop).FieldType.Name;
                        if (propertyTypeName.Contains("Nullable"))
                        {
                            isNullable = true;
                            propertyTypeName = ((FieldInfo)prop).FieldType.GenericTypeArguments.Select(x => x.Name).FirstOrDefault();
                        }
                    }

                    switch (propertyTypeName)
                    {
                        case "String[]":
                            defFieldType = RepositorySchema.DataTypeConstants.List;
                            isNullable = true;
                            break;
                        case "String":
                            defFieldType = RepositorySchema.DataTypeConstants.String;
                            isNullable = true;
                            break;
                        case "Int16":
                        case "Int32":
                        case "UInt32":
                        case "Byte":
                            defFieldType = RepositorySchema.DataTypeConstants.Int;
                            break;
                        case "Int64":
                            defFieldType = RepositorySchema.DataTypeConstants.Int64;
                            break;
                        case "Bool":
                        case "Boolean":
                            defFieldType = RepositorySchema.DataTypeConstants.Bool;
                            break;
                        case "DateTime":
                            defFieldType = RepositorySchema.DataTypeConstants.DateTime;
                            break;
                        case "Double":
                        case "Single":
                        case "Decimal":
                            defFieldType = RepositorySchema.DataTypeConstants.Float;
                            break;
                        case "Char":
                            defFieldType = RepositorySchema.DataTypeConstants.String;
                            defLength = 1;
                            break;
                        case "GeoCode":
                            defFieldType = RepositorySchema.DataTypeConstants.GeoCode;
                            isNullable = true;
                            break;
                        default:
                            break;
                    }

                    var name = GetAttributeValue<string>(field, "Name", prop.Name);
                    var dataType = GetAttributeValue<RepositorySchema.DataTypeConstants>(field, "DataType", defFieldType);
                    var fieldType = GetAttributeValue<RepositorySchema.FieldTypeConstants>(field, "FieldType", RepositorySchema.FieldTypeConstants.Field);
                    var isPrimaryKey = GetAttributeValue<bool>(field, "IsPrimaryKey", false);
                    var isDataGrouping = GetAttributeValue<bool>(field, "IsDataGrouping", false);
                    var length = GetAttributeValue<int>(field, "Length", defLength);
                    var allowTextSearch = GetAttributeValue<bool>(field, "AllowTextSearch", false);
                    var isPivot = GetAttributeValue<bool>(field, "IsPivot", false);
                    var pivotGroup = GetAttributeValue<string>(field, "PivotGroup", String.Empty);
                    var pivotOrder = GetAttributeValue<int>(field, "PivotOrder", 0);
                    var description = GetAttributeValue<string>(field, "Description", String.Empty);
                    var searchAsc = GetAttributeValue<bool>(field, "SearchAsc", true);
                    var allowIndex = GetAttributeValue<bool>(field, "AllowIndex", true);

                    if (fieldType == RepositorySchema.FieldTypeConstants.Dimension)
                        fieldDefition = new DimensionDefinition();
                    else
                        fieldDefition = new FieldDefinition();

                    fieldDefition.Name = name;
                    fieldDefition.DataType = dataType;
                    fieldDefition.FieldType = fieldType;
                    fieldDefition.IsPrimaryKey = isPrimaryKey;
                    fieldDefition.IsDataGrouping = isDataGrouping;
                    fieldDefition.AllowNull = isNullable;
                    fieldDefition.Length = length;
                    fieldDefition.AllowTextSearch = allowTextSearch;
                    fieldDefition.Name = name;
                    fieldDefition.IsPivot = isPivot;
                    fieldDefition.PivotGroup = pivotGroup;
                    fieldDefition.PivotOrder = pivotOrder;
                    fieldDefition.Description = description;
                    fieldDefition.SearchAsc = searchAsc;
                    fieldDefition.AllowIndex = allowIndex;

                    //If PK and nullable then error
                    if (isPrimaryKey && isNullable)
                    {
                        throw new Exception("The primary key cannot be nullable.");
                    }

                    if (isPrimaryKey && !allowIndex)
                    {
                        throw new Exception("The primary key must be indexed.");
                    }

                    //Default to unlimited string length
                    if (defFieldType == RepositorySchema.DataTypeConstants.String && defLength < 0)
                    {
                        defLength = 0;
                    }

                    if (fieldType == RepositorySchema.FieldTypeConstants.Dimension)
                    {
                        switch (defFieldType)
                        {
                            case RepositorySchema.DataTypeConstants.Bool:
                            case RepositorySchema.DataTypeConstants.String:
                            case RepositorySchema.DataTypeConstants.List:
                            case RepositorySchema.DataTypeConstants.Int:
                            case RepositorySchema.DataTypeConstants.Int64:
                            case RepositorySchema.DataTypeConstants.DateTime:
                                break;
                            default:
                                throw new Exception($"Field '{name}': The data type '{defFieldType}' cannot be a dimension.");
                        }
                    }

                    if (fieldType == RepositorySchema.FieldTypeConstants.Dimension)
                    {
                        var dimension = fieldDefition as DimensionDefinition;
                        if (dimension == null)
                            throw new Exception("Dimension is null");

                        var didx = GetAttributeValue<int?>(field, "Didx", null);
                        if (didx.HasValue)
                            dimension.DIdx = didx.Value;

                        dimension.Parent = GetAttributeValue<string>(field, "Parent", String.Empty);
                        dimension.DimensionType = GetAttributeValue<RepositorySchema.DimensionTypeConstants>(field, "DimensionType", RepositorySchema.DimensionTypeConstants.Normal);
                        var numericBreak = GetAttributeValue<long?>(field, "NumericBreak", null);
                        if ((dimension.DataType == RepositorySchema.DataTypeConstants.Int || dimension.DataType == RepositorySchema.DataTypeConstants.Int64) && numericBreak.HasValue)
                        {
                            dimension.NumericBreak = numericBreak.Value;
                        }
                        else if (numericBreak.HasValue)
                        {
                            throw new Exception("Numeric breaks are only applicable for Integer fields.");
                        }

                        //If the datatype is list then the DimensionType must be list
                        if (dimension.DataType == RepositorySchema.DataTypeConstants.List)
                            dimension.DimensionType = RepositorySchema.DimensionTypeConstants.List;
                    }

                    if (GetAttributeValue<bool>(field, "UserPermission", false))
                    {
                        schema.UserPermissionField = fieldDefition;
                    }

                    schema.FieldList.Add(fieldDefition);
                }
            }
            #endregion

            var hasGeo = schema.FieldList.Any(x => x.DataType == RepositorySchema.DataTypeConstants.GeoCode);
            var hasGroupingData = (schema.FieldList.Where(x => x.IsDataGrouping).Count() > 1);

            if (schema.FieldList.Count == 0)
                throw new Exception("The item must have fields defined.");
            if (schema.FieldList.Count(x => x.IsPrimaryKey) != 1)
                throw new Exception("The item must have exactly one primary key defined.");
            if (schema.FieldList.Count != schema.FieldList.Select(x => x.Name.ToLower()).Distinct().Count())
                throw new Exception("The item cannot have duplicate fields.");
            if (schema.FieldList.Where(x => x.IsDataGrouping).Count() > 1)
                throw new Exception("The item cannot have more than one data group field.");
            if (hasGeo && hasGroupingData)
                throw new Exception("The item cannot have a data grouping and GeoCode field.");

            //This is a child so remove parent fields from schema
            if (schema.ParentID != null && !inheritedFieldList)
            {
                if (!dsType.BaseType.GetInterfaces().Any(x => x.Name == typeof(IDatastoreItem).Name))
                    throw new Exception($"The item base must implement {typeof(IDatastoreItem).Name}.");
                var parentSchema = LoadSchemaForType(dsType.BaseType);
                schema = schema.Subtract(parentSchema);
            }

            foreach (var ditem in schema.DimensionList.Where(x => !string.IsNullOrEmpty(x.Parent)).ToList())
            {
                //Verify that no parent dimension is self-referential
                if (ditem.Parent.Match(ditem.Name))
                    throw new Exception($"The dimension '{ditem.Name}' cannot be its own parent.");

                //Error check that parent dimension actually exists
                if (!schema.DimensionList.Any(x => x.Name == ditem.Parent))
                    throw new Exception($"The dimension '{ditem.Name}' defines a non-existent parent.");
            }

            //TODO: Verify that no parent dimensions cause a cycle (A->B->A)

            return schema;
        }
        
        private static TResult GetAttributeValue<TResult>(CustomAttributeData customAttribute, string name, object defaultValue)
        {
            var value = customAttribute.NamedArguments
                .Where(x => name.Equals(x.MemberName, StringComparison.OrdinalIgnoreCase))
                .Select(x => x.TypedValue.Value)
                .FirstOrDefault();

            if (value == null)
                return (TResult)defaultValue;

            if (typeof(TResult) == typeof(Guid))
                value = Guid.Parse((string)value);
            else if (typeof(TResult) == typeof(DateTime))
                value = DateTime.Parse((string)value);

            return (TResult)value;
        }

        internal static List<CustomAttributeData> GetTemplateFields(Type sourceType)
        {
            return sourceType.GetProperties()
                .Where(x => x.CustomAttributes.Any(a => a.AttributeType == typeof(DatastoreFieldAttribute)))
                .Select(x => x.CustomAttributes.First(a => a.AttributeType == typeof(DatastoreFieldAttribute)))
                .ToList();
        }

        internal static List<String> GetTemplateFieldNames(Type sourceType)
        {
            var properties = GetTemplateFields(sourceType);

            var propertyNames = properties
                .Select(y => y.NamedArguments
                    .Where(s => s.MemberName == "Name")
                    .Select(o => (string)o.TypedValue.Value)
                    .First()).ToList();

            return propertyNames;
        }

        internal bool IsFieldsetValidForType(FieldDefinition[] fieldset, Type sourceType)
        {
            if (fieldset == null)
                return false;

            var templateFields = GetTemplateFieldNames(sourceType);
            if (templateFields == null)
                return false;

            var templateCount = templateFields.Count();
            if (templateCount == 0)
                return false;

            if (fieldset.Count() < templateCount)
                return false;

            var hashSet = new HashSet<string>(fieldset.Select(x => x.Name));

            return templateFields.All(x => hashSet.Contains(x));
        }

        #region IDisposable Support
        /// <summary />
        protected virtual void Dispose(bool disposing)
        {
            if (this.DataModelService is IClientChannel channel)
            {
                try
                {
                    if (channel.State != CommunicationState.Faulted)
                        channel.Close();
                }
                catch (Exception)
                {
                    //Do Nothing
                    channel.Abort();
                }
                finally
                {
                    if (channel.State != CommunicationState.Closed)
                        channel.Abort();
                    channel = null;
                }
            }
            this.DataModelService = null;

            if (ChannelFactory != null)
            {
                try
                {
                    if (ChannelFactory.State != CommunicationState.Faulted)
                        ChannelFactory.Close();
                }
                catch (Exception)
                {
                    //Do Nothing
                    ChannelFactory.Abort();
                }
                finally
                {
                    if (ChannelFactory.State != CommunicationState.Closed)
                        ChannelFactory.Abort();
                    ChannelFactory = null;
                }
            }

        }

        /// <summary />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
