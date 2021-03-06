using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    public class DatastoreRepository
    {
        /// <summary />
        public static RepositorySchema LoadSchemaTemplate(Type datastoreType)
        {
            var schema = DatastoreService.LoadSchemaForType(datastoreType);
            schema.ID = schema.ID; //RepositoryId;
            schema.Name = string.Format("{0}-{1}", schema.Name[0], schema.ID); //RepositoryId
            schema.CreatedDate = DateTime.Now;
            return schema;
        }
    }

    /// <summary>
    ///  A DatastoreService instance represents a combination of the Unit Of Work and Repository
    ///     patterns such that it can be used to query from a repository and group together
    ///     changes that will then be written back to the store as a unit.
    /// </summary>
    public class DatastoreRepository<TSourceType> : DatastoreRepository, IDisposable, IDatastoreRepository, IDatastoreRepository<TSourceType>
        where TSourceType : IDatastoreItem
    {
        private DatastoreService _datastoreService;

        //Keep track of when this instance was created for debugging
        private DateTime _instanceCreated = DateTime.Now;

        /// <summary />
        public DatastoreRepository(Guid repositoryId)
            : this(repositoryId, "localhost")
        {

        }

        /// <summary />
        public DatastoreRepository(Guid repositoryId, string serverName)
            : base()
        {
            if (string.IsNullOrEmpty(serverName))
                throw new Exception("Server not set");

            var arr = serverName.Split(':');
            if (arr.Length > 2)
                throw new Exception("Server not set");

            var port = 1973;
            if (arr.Length == 2)
            {
                port = arr[1].ToInt();
                serverName = arr[0];
            }

            //If configured for failover then grab the current server
            if (serverName == "@config")
            {
                if (FailoverConfiguration.CurrentServer != null)
                {
                    serverName = FailoverConfiguration.CurrentServer.Server;
                    port = FailoverConfiguration.CurrentServer.Port;
                }
                else
                    throw new Exception("Cannot find a configured server");
            }
            _datastoreService = new DatastoreService(repositoryId, serverName, port);
        }

        /// <summary>
        ///  A DatastoreService instance represents a combination of the Unit Of Work and Repository
        ///     patterns such that it can be used to query from a repository and group together
        ///     changes that will then be written back to the store as a unit.
        /// </summary>
        public DatastoreRepository(Guid repositoryId, string serverName, int port)
            : this(repositoryId, serverName + ":" + port)
        {
        }

        /// <summary />
        ~DatastoreRepository()
        {
            Dispose(false);
        }

        /// <summary>
        /// The current connected server
        /// </summary>
        public string Server
        {
            get { return $"{_datastoreService.ServerName}:{_datastoreService.Port}"; }
        }

        /// <summary />
        public virtual DateTime InstanceCreated => _instanceCreated;

        /// <summary>
        /// A unit work object that allows you to query a repository
        /// </summary>
        public virtual IDatastoreQueryable<TSourceType> Query
        {
            get { return new DatastoreQueryable<TSourceType>(_datastoreService); }
        }

        /// <summary>
        /// A unit work object that allows you to queue update operations
        /// </summary>
        public virtual IDatastoreUpdatable<TSourceType> Update
        {
            get { return new DatastoreUpdatable<TSourceType>(_datastoreService); }
        }

        /// <summary>
        /// A unit work object that allows you to queue deletions
        /// </summary>
        public virtual IDatastoreDeletable<TSourceType> Delete
        {
            get { return new DatastoreDeletable<TSourceType>(_datastoreService); }
        }

        /// <summary>
        /// A unit work object that allows you to slice a repository
        /// </summary>
        public virtual IDatastoreSliceable<TSourceType> Slice
        {
            get { return new DatastoreSliceable<TSourceType>(_datastoreService); }
        }

        /// <summary>
        /// The key of the current repository
        /// </summary>
        public virtual Guid RepositoryId
        {
            get { return _datastoreService.RepositoryId; }
        }

        /// <summary>
        /// Verifies that the repository is online
        /// </summary>
        public virtual bool IsValid()
        {
            // Would a count query be better than pulling 1 record?
            var query = new DataQuery();
            query.PageOffset = 0;
            query.RecordsPerPage = 1;
            query.UseDefaults = false;
            query.ExcludeCount = true;
            query.IncludeRecords = true;
            query.IncludeDimensions = false;

            var results = _datastoreService.Query(query);
            return _datastoreService.IsFieldsetValidForType(results.Fieldset, typeof(TSourceType));
        }

        /// <summary>
        /// Determines if the service is responding
        /// </summary>
        public virtual bool IsServerAlive()
        {
            return _datastoreService.IsServerAlive();
        }

        /// <summary>
        /// Determines if the service is the master
        /// </summary>
        public virtual bool IsServerMaster()
        {
            return _datastoreService.IsServerMaster();
        }

        /// <summary>
        /// Resets the service to initial load state to ensure it is ready to accept requests.
        /// This should be called right before a fail over to this service.
        /// </summary>
        public virtual bool ResetMaster()
        {
            return _datastoreService.ResetMaster();
        }

        /// <summary>
        /// Determines if the specified repository exists
        /// </summary>
        public virtual bool RepositoryExists()
        {
            return _datastoreService.RepositoryExists();
        }

        /// <summary>
        /// Deletes the specified repository
        /// </summary>
        public virtual bool DeleteRepository()
        {
            return _datastoreService.DeleteRepository();
        }

        /// <summary>
        /// Deletes all items from the specified repository
        /// </summary>
        public virtual ActionDiagnostics ClearRepository()
        {
            return _datastoreService.ClearRepository();
        }

        /// <summary>
        /// Creates the specified repository
        /// </summary>
        public virtual void CreateRepository(string name = null)
        {
            var schema = LoadSchemaTemplate();
            schema.ID = this.RepositoryId;  
            if (!string.IsNullOrEmpty(name))
                schema.Name = name;
            _datastoreService.CreateRepository(schema);
        }

        /// <summary>
        /// Creates the specified repository
        /// </summary>
        public virtual void CreateRepository(Guid repositoryid, string name = null)
        {
            var schema = LoadSchemaTemplate();
            schema.ID = repositoryid;
            schema.Name = name ?? string.Empty;
            _datastoreService.CreateRepository(schema);
        }

        /// <summary>
        /// Creates a schema object from a user defined class that implements the IDatastoreItem interface
        /// </summary>
        public virtual RepositorySchema LoadSchemaTemplate()
        {
            var schema = DatastoreService.LoadSchemaTemplate(typeof(TSourceType));
            schema.ID = this.RepositoryId;
            return schema;
        }

        /// <summary>
        /// Gets a schema object for the repository
        /// </summary>
        public virtual RepositorySchema GetSchema()
        {
            return _datastoreService.Schema;
        }

        /// <summary>
        /// Returns a number that represents a unique data version.
        /// Any time data changes in the repository for any reason, this number changes.
        /// </summary>
        /// <returns></returns>
        public virtual long GetDataVersion()
        {
            return _datastoreService.GetDataVersion();
        }

        /// <summary>
        /// Given a store object type, this method will update the repository schema to match this object.
        /// </summary>
        public virtual ActionDiagnostics UpdateSchema(Type datastoreType)
        {
            var schema = DatastoreService.LoadSchemaTemplate(datastoreType);
            schema.ID = this.RepositoryId;
            return this.UpdateSchema(schema);
        }

        /// <summary>
        /// Given a store object type, this method will update the repository schema to match this object.
        /// </summary>
        public virtual ActionDiagnostics UpdateSchema(Type datastoreType, Guid repositoryId)
        {
            var schema = DatastoreService.LoadSchemaTemplate(datastoreType);
            schema.ID = repositoryId;
            return this.UpdateSchema(schema);
        }

        /// <summary>
        /// Given a store object type, this method will update the repository schema to match this object.
        /// </summary>
        public virtual ActionDiagnostics UpdateSchema(RepositorySchema schema)
        {
            return _datastoreService.UpdateSchema(schema);
        }

        /// <summary>
        /// Resets the schema to a user defined one.
        /// This is used when the schema has been modifed at runtime and the POCO object does not contains all schema fields.
        /// </summary>
        public virtual void ResetSchema(RepositorySchema schema)
        {
            _datastoreService.Schema = schema;
        }

        /// <summary>
        /// Adds an item to the repository
        /// </summary>
        public virtual ActionDiagnostics InsertOrUpdate(TSourceType item)
        {
            return InsertOrUpdate(new List<TSourceType>() { item });
        }

        /// <summary>
        /// Adds a list of items to the repository
        /// </summary>
        public virtual ActionDiagnostics InsertOrUpdate(List<TSourceType> itemsToInsert)
        {
            var genericType = typeof(TSourceType);
            var schema = _datastoreService.Schema;

            if (schema == null)
                throw new Exception("Schema is null");
            if (schema.FieldList == null)
                throw new Exception("Schema FieldList is null");

            int count = schema.FieldList.Count();
            var dataItems = new List<DataItem>();
            foreach (var item in itemsToInsert)
            {
                var dataStoreItem = item as IDatastoreItem;
                if (dataStoreItem == null)
                    throw new Exception("The item must implement " + typeof(IDatastoreItem).Name + ".");

                var dataItem = new DataItem();
                var extraCount = 0;
                if (dataStoreItem.ExtraValues != null)
                    extraCount = dataStoreItem.ExtraValues.Count;

                dataItem.ItemArray = new object[count + extraCount];

                var index = 0;
                foreach (var field in schema.FieldList)
                {
                    var propertyInfo = genericType.GetProperty(field.Name);
                    if (propertyInfo != null)
                    {
                        dataItem.ItemArray[index] = propertyInfo.GetValue(item);
                    }
                    else
                    {
                        if (dataStoreItem != null && dataStoreItem.ExtraValues != null)
                        {
                            if (dataStoreItem.ExtraValues.ContainsKey(field.Name))
                            {
                                dataItem.ItemArray[index] = dataStoreItem.ExtraValues[field.Name];
                            }
                        }
                        else
                        {
                            dataItem.ItemArray[index] = null;
                        }
                    }
                    index++;
                }

                dataItems.Add(dataItem);
            }

            return _datastoreService.UpdateData(dataItems);
        }

        /// <summary>
        /// Queues an item to be added to the repository asynchronously and returns immediately
        /// </summary>
        public async Task<ActionDiagnostics> InsertOrUpdateAsync(TSourceType item)
        {
            return await Task<int>.Run(() =>
            {
                return InsertOrUpdate(item);
            });
        }

        /// <summary>
        /// Queues a list of items to be added to the repository asynchronously and returns immediately
        /// </summary>
        public async Task<ActionDiagnostics> InsertOrUpdateAsync(List<TSourceType> itemsToInsert)
        {
            return await Task<int>.Run(() =>
            {
                return InsertOrUpdate(itemsToInsert);
            });
        }

        /// <summary />
        public virtual void AddPermissions(IEnumerable<PermissionItem> list)
        {
            _datastoreService.AddPermissions(list);
        }

        /// <summary />
        public virtual void ClearPermissions(string fieldValue)
        {
            _datastoreService.ClearPermissions(fieldValue);
        }

        /// <summary />
        public virtual void ClearPermissions(int? fieldValue)
        {
            if (fieldValue == null) _datastoreService.ClearPermissions(null);
            else _datastoreService.ClearPermissions(fieldValue.ToString());
        }

        /// <summary />
        public virtual void ClearUserPermissions(int userId)
        {
            _datastoreService.ClearUserPermissions(userId);
        }

        /// <summary />
        public virtual void DeletePermissions(IEnumerable<PermissionItem> list)
        {
            _datastoreService.DeletePermissions(list);
        }

        /// <summary />
        public virtual bool ResetDimensionValue(long dvidx, string value)
        {
            return _datastoreService.ResetDimensionValue(dvidx, value);
        }

        /// <summary />
        public virtual bool DeleteDimensionValue(long dvidx)
        {
            return _datastoreService.DeleteDimensionValue(dvidx);
        }

        /// <summary />
        public virtual int GetTimestamp()
        {
            return _datastoreService.GetTimestamp();
        }

        /// <summary>
        /// The time in milliseconds that this object has been in existence
        /// </summary>
        public virtual long ObjectLifetime => (long)DateTime.Now.Subtract(this.InstanceCreated).TotalMilliseconds;

        #region IDisposable Support
        /// <summary />
        protected virtual void Dispose(bool disposing)
        {
            try
            {
                if (_datastoreService != null)
                {
                    _datastoreService.Dispose();
                    _datastoreService = null;
                }
            }
            catch { }
        }

        /// <summary />
        void IDisposable.Dispose()
        {
            try
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }
            catch { }
        }
        #endregion
    }
}